from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
import asyncio
import secrets
import os

router = APIRouter(prefix="/api/stream", tags=["streaming"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

rooms: dict = {}
teacher_tokens: dict = {}
room_lock = asyncio.Lock()


class CreateRoomRequest(BaseModel):
    teacher_name: str
    room_name: Optional[str] = None
    password: Optional[str] = None


class TeacherLoginRequest(BaseModel):
    teacher_name: str
    password: str


class JoinRoomRequest(BaseModel):
    room_id: str
    student_name: Optional[str] = None


def _load_teacher_credentials() -> dict:
    raw = os.environ.get("TEACHER_CREDENTIALS", "")
    creds: dict = {}
    if not raw:
        return creds
    for pair in raw.split(","):
        if ":" in pair:
            user, pwd = pair.split(":", 1)
            creds[user.strip()] = pwd.strip()
    return creds


def verify_teacher(username: str, password: str) -> bool:
    creds = _load_teacher_credentials()
    if not creds:
        return False
    stored = creds.get(username)
    if stored and stored == password:
        return True
    return False


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def validate_token(token: str) -> Optional[dict]:
    info = teacher_tokens.get(token)
    if not info:
        return None
    if datetime.now() > info["expires_at"]:
        teacher_tokens.pop(token, None)
        return None
    return info


@router.post("/auth/login")
async def teacher_login(data: TeacherLoginRequest):
    if not verify_teacher(data.teacher_name, data.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = generate_token()
    teacher_tokens[token] = {
        "teacher_name": data.teacher_name,
        "created_at": datetime.now().isoformat(),
        "expires_at": datetime.now() + timedelta(hours=8),
    }

    return {
        "status": "authenticated",
        "token": token,
        "teacher_name": data.teacher_name,
    }


@router.post("/auth/verify")
async def verify_token_endpoint(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return {"status": "valid", "teacher_name": info["teacher_name"]}


@router.post("/room/create")
async def create_room(data: CreateRoomRequest, request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    room_id = secrets.token_urlsafe(8)
    room_name = data.room_name or f"{info['teacher_name']}'s Room"

    async with room_lock:
        rooms[room_id] = {
            "room_id": room_id,
            "room_name": room_name,
            "teacher_name": info["teacher_name"],
            "teacher_token": token,
            "created_at": datetime.now().isoformat(),
            "password": data.password,
            "teacher_ws": None,
            "student_connections": {},
            "is_live": False,
            "stream_type": None,
        }

    return {
        "status": "created",
        "room_id": room_id,
        "room_name": room_name,
        "join_url": f"/stream/watch/{room_id}",
    }


@router.get("/room/{room_id}/info")
async def room_info(room_id: str):
    async with room_lock:
        room = rooms.get(room_id)
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")

    return {
        "room_id": room["room_id"],
        "room_name": room["room_name"],
        "teacher_name": room["teacher_name"],
        "is_live": room["is_live"],
        "stream_type": room["stream_type"],
        "viewer_count": len(room["student_connections"]),
        "requires_password": room["password"] is not None,
    }


@router.get("/rooms/active")
async def list_active_rooms():
    active = []
    async with room_lock:
        for rid, room in rooms.items():
            active.append({
                "room_id": rid,
                "room_name": room["room_name"],
                "teacher_name": room["teacher_name"],
                "is_live": room["is_live"],
                "stream_type": room["stream_type"],
                "viewer_count": len(room["student_connections"]),
                "requires_password": room["password"] is not None,
            })
    return {"rooms": active}


@router.post("/room/{room_id}/close")
async def close_room(room_id: str, request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    async with room_lock:
        room = rooms.get(room_id)
        if not room:
            raise HTTPException(status_code=404, detail="Room not found")
        if room["teacher_token"] != token:
            raise HTTPException(status_code=403, detail="Not your room")

        for sid, ws in room["student_connections"].items():
            try:
                await ws.send_json({"type": "room_closed"})
                await ws.close()
            except Exception:
                pass

        del rooms[room_id]

    return {"status": "closed"}


@router.websocket("/ws/teacher/{room_id}")
async def teacher_websocket(websocket: WebSocket, room_id: str, token: str = ""):
    info = validate_token(token)
    if not info:
        await websocket.close(code=4001, reason="Invalid token")
        return

    async with room_lock:
        room = rooms.get(room_id)
        if not room:
            await websocket.close(code=4004, reason="Room not found")
            return
        if room["teacher_token"] != token:
            await websocket.close(code=4003, reason="Not your room")
            return

    await websocket.accept()

    async with room_lock:
        rooms[room_id]["teacher_ws"] = websocket
        rooms[room_id]["is_live"] = True

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "offer":
                target = data.get("target")
                async with room_lock:
                    student_ws = rooms[room_id]["student_connections"].get(target)
                if student_ws:
                    await student_ws.send_json({
                        "type": "offer",
                        "sdp": data.get("sdp"),
                        "source": "teacher",
                        "stream_kind": data.get("stream_kind", "camera"),
                    })

            elif msg_type == "answer":
                target = data.get("target")
                async with room_lock:
                    student_ws = rooms[room_id]["student_connections"].get(target)
                if student_ws:
                    await student_ws.send_json({
                        "type": "answer",
                        "sdp": data.get("sdp"),
                        "source": "teacher",
                    })

            elif msg_type == "ice_candidate":
                target = data.get("target")
                if target:
                    async with room_lock:
                        student_ws = rooms[room_id]["student_connections"].get(target)
                    if student_ws:
                        await student_ws.send_json({
                            "type": "ice_candidate",
                            "candidate": data.get("candidate"),
                            "source": "teacher",
                        })
                else:
                    async with room_lock:
                        students = dict(rooms[room_id]["student_connections"])
                    for sid, sws in students.items():
                        try:
                            await sws.send_json({
                                "type": "ice_candidate",
                                "candidate": data.get("candidate"),
                                "source": "teacher",
                            })
                        except Exception:
                            pass

            elif msg_type == "stream_update":
                async with room_lock:
                    rooms[room_id]["stream_type"] = data.get("stream_type")
                    students = dict(rooms[room_id]["student_connections"])
                for sid, sws in students.items():
                    try:
                        await sws.send_json({
                            "type": "stream_update",
                            "stream_type": data.get("stream_type"),
                        })
                    except Exception:
                        pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"Teacher WS error: {e}")
    finally:
        async with room_lock:
            if room_id in rooms:
                rooms[room_id]["teacher_ws"] = None
                rooms[room_id]["is_live"] = False
                students = dict(rooms[room_id]["student_connections"])
                for sid, sws in students.items():
                    try:
                        await sws.send_json({"type": "teacher_disconnected"})
                    except Exception:
                        pass


@router.websocket("/ws/student/{room_id}")
async def student_websocket(
    websocket: WebSocket, room_id: str, student_id: str = "", password: str = ""
):
    async with room_lock:
        room = rooms.get(room_id)
        if not room:
            await websocket.close(code=4004, reason="Room not found")
            return
        if room["password"] and room["password"] != password:
            await websocket.close(code=4003, reason="Wrong password")
            return

    if not student_id:
        student_id = secrets.token_urlsafe(6)

    await websocket.accept()

    async with room_lock:
        rooms[room_id]["student_connections"][student_id] = websocket
        teacher_ws = rooms[room_id]["teacher_ws"]
        is_live = rooms[room_id]["is_live"]

    await websocket.send_json({
        "type": "joined",
        "student_id": student_id,
        "is_live": is_live,
    })

    if teacher_ws and is_live:
        try:
            await teacher_ws.send_json({
                "type": "student_joined",
                "student_id": student_id,
            })
        except Exception:
            pass

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "offer":
                async with room_lock:
                    teacher_ws = rooms[room_id].get("teacher_ws")
                if teacher_ws:
                    await teacher_ws.send_json({
                        "type": "offer",
                        "sdp": data.get("sdp"),
                        "source": student_id,
                    })

            elif msg_type == "answer":
                async with room_lock:
                    teacher_ws = rooms[room_id].get("teacher_ws")
                if teacher_ws:
                    await teacher_ws.send_json({
                        "type": "answer",
                        "sdp": data.get("sdp"),
                        "source": student_id,
                    })

            elif msg_type == "ice_candidate":
                async with room_lock:
                    teacher_ws = rooms[room_id].get("teacher_ws")
                if teacher_ws:
                    await teacher_ws.send_json({
                        "type": "ice_candidate",
                        "candidate": data.get("candidate"),
                        "source": student_id,
                    })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"Student WS error: {e}")
    finally:
        async with room_lock:
            if room_id in rooms:
                rooms[room_id]["student_connections"].pop(student_id, None)
                teacher_ws = rooms[room_id].get("teacher_ws")
        if teacher_ws:
            try:
                await teacher_ws.send_json({
                    "type": "student_left",
                    "student_id": student_id,
                })
            except Exception:
                pass

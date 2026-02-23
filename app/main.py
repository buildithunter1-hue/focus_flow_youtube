"""
Focus Tracker Backend - Session-Based Architecture (No WebRTC)
Production-ready for 500+ concurrent students
Teachers create sessions with content URLs; students watch embedded content
with local webcam analytics (FaceMesh/expressions).
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel
import httpx
import asyncio
import uuid
import secrets
import os
import re

API_BASE = "https://davisEmailconfigureApi.softsolanalytics.com/API/User"

limiter = Limiter(key_func=get_remote_address)

students_data: dict = {}
student_timelines: dict = {}
data_lock = asyncio.Lock()

# Session storage (in-memory)
content_sessions: dict = {}
session_lock = asyncio.Lock()

# Teacher auth tokens
teacher_tokens: dict = {}

http_client: Optional[httpx.AsyncClient] = None


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


def detect_content_type(url: str) -> str:
    """Auto-detect content type from URL."""
    url_lower = url.lower().strip()

    if re.search(r'(youtube\.com|youtu\.be)', url_lower):
        return 'youtube'
    if re.search(r'vimeo\.com', url_lower):
        return 'vimeo'
    if url_lower.endswith('.m3u8') or 'hls' in url_lower:
        return 'hls'
    if re.search(r'\.(mp4|webm|ogg|mov)(\?|$)', url_lower):
        return 'mp4'

    # Default: try as direct video
    return 'mp4'


def extract_youtube_id(url: str) -> Optional[str]:
    """Extract YouTube video ID from various URL formats."""
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def extract_vimeo_id(url: str) -> Optional[str]:
    """Extract Vimeo video ID from URL."""
    match = re.search(r'vimeo\.com/(\d+)', url)
    if match:
        return match.group(1)
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=200)
    )
    print("=" * 60)
    print("FOCUS TRACKER - Session-Based Architecture (No WebRTC)")
    print("=" * 60)
    print("Features:")
    print("  - Session-based content sharing (YouTube/Vimeo/MP4/HLS)")
    print("  - No media server cost (video from YouTube/Vimeo/CDN)")
    print("  - Local webcam analytics (FaceMesh/expressions)")
    print("  - Async API calls with connection pooling")
    print("  - Rate limiting (100 req/min per IP)")
    print("  - Scalable for 500+ students")
    print("=" * 60)
    yield
    await http_client.aclose()


app = FastAPI(title="Focus Tracker API", version="3.0.0", lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Disable CORS. Do not remove this for full-stack development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# -- Pydantic Models --

class TeacherLoginRequest(BaseModel):
    teacher_name: str
    password: str


class CreateSessionRequest(BaseModel):
    session_name: Optional[str] = None
    content_url: str
    content_type: Optional[str] = None


class StudentJoinRequest(BaseModel):
    student_id: str
    name: Optional[str] = None
    session_id: Optional[str] = None


class StudentUpdateRequest(BaseModel):
    student_id: str
    name: Optional[str] = None
    guid: Optional[str] = None
    session_id: Optional[str] = None
    attention_score: int = 0
    emotion: str = "neutral"
    engagement_level: str = "low"
    face_present_ratio: float = 0.0
    gaze_on_screen_ratio: float = 0.0
    blink_rate: int = 0


class StudentLeaveRequest(BaseModel):
    student_id: str
    name: Optional[str] = None
    session_id: Optional[str] = None


# -- Helpers --

async def cleanup_inactive_students():
    cutoff = datetime.now() - timedelta(minutes=10)
    async with data_lock:
        inactive = [
            sid for sid, s in students_data.items()
            if datetime.fromisoformat(s.get('last_update', s['joined_at'])) < cutoff
        ]
        for sid in inactive:
            students_data.pop(sid, None)
            student_timelines.pop(sid, None)


async def add_to_timeline(student_id: str, data: dict):
    async with data_lock:
        if student_id not in student_timelines:
            student_timelines[student_id] = []

        student_timelines[student_id].append({
            'timestamp': datetime.now().isoformat(),
            'attention': data.get('attention_score', 0),
            'emotion': data.get('emotion', 'neutral'),
            'engagement': data.get('engagement_level', 'low'),
            'face_present_ratio': data.get('face_present_ratio', 0.0),
            'gaze_on_screen_ratio': data.get('gaze_on_screen_ratio', 0.0),
            'session_id': data.get('session_id')
        })

        if len(student_timelines[student_id]) > 200:
            student_timelines[student_id] = student_timelines[student_id][-200:]


# -- Teacher Auth Endpoints --

@app.post("/api/auth/login")
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


@app.post("/api/auth/verify")
async def verify_token_endpoint(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return {"status": "valid", "teacher_name": info["teacher_name"]}


# -- Session Endpoints --

@app.post("/api/session/create")
async def create_session(data: CreateSessionRequest, request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    session_id = secrets.token_urlsafe(8)
    content_type = data.content_type or detect_content_type(data.content_url)
    session_name = data.session_name or f"{info['teacher_name']}'s Session"

    embed_info: dict = {"content_type": content_type, "content_url": data.content_url}
    if content_type == "youtube":
        video_id = extract_youtube_id(data.content_url)
        if video_id:
            embed_info["embed_url"] = f"https://www.youtube.com/embed/{video_id}?rel=0&modestbranding=1&enablejsapi=1"
            embed_info["video_id"] = video_id
        else:
            embed_info["embed_url"] = data.content_url
    elif content_type == "vimeo":
        video_id = extract_vimeo_id(data.content_url)
        if video_id:
            embed_info["embed_url"] = f"https://player.vimeo.com/video/{video_id}?title=0&byline=0&portrait=0"
            embed_info["video_id"] = video_id
        else:
            embed_info["embed_url"] = data.content_url

    async with session_lock:
        content_sessions[session_id] = {
            "session_id": session_id,
            "session_name": session_name,
            "content_url": data.content_url,
            "content_type": content_type,
            "embed_info": embed_info,
            "teacher_name": info["teacher_name"],
            "teacher_token": token,
            "created_at": datetime.now().isoformat(),
            "is_active": True,
            "student_count": 0,
            "students": [],
        }

    share_url = f"/session/{session_id}"

    return {
        "status": "created",
        "session_id": session_id,
        "session_name": session_name,
        "share_url": share_url,
        "content_type": content_type,
        "embed_info": embed_info,
    }


@app.get("/api/session/{session_id}/info")
async def session_info(session_id: str):
    async with session_lock:
        session = content_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "session_id": session["session_id"],
        "session_name": session["session_name"],
        "content_url": session["content_url"],
        "content_type": session["content_type"],
        "embed_info": session["embed_info"],
        "teacher_name": session["teacher_name"],
        "is_active": session["is_active"],
        "student_count": session["student_count"],
        "created_at": session["created_at"],
    }


@app.get("/api/sessions/active")
async def list_active_sessions():
    active = []
    async with session_lock:
        for sid, session in content_sessions.items():
            if session["is_active"]:
                active.append({
                    "session_id": sid,
                    "session_name": session["session_name"],
                    "teacher_name": session["teacher_name"],
                    "content_type": session["content_type"],
                    "student_count": session["student_count"],
                    "created_at": session["created_at"],
                })
    return {"sessions": active}


@app.post("/api/session/{session_id}/close")
async def close_session(session_id: str, request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth.split(" ", 1)[1]
    info = validate_token(token)
    if not info:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    async with session_lock:
        session = content_sessions.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        if session["teacher_token"] != token:
            raise HTTPException(status_code=403, detail="Not your session")
        session["is_active"] = False

    return {"status": "closed"}


# -- Page Routes --

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "version": "3.0.0"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("teacher_dashboard.html", {"request": request})


@app.get("/teacher/analytics", response_class=HTMLResponse)
async def teacher_analytics_page(request: Request):
    return templates.TemplateResponse("teacher_analytics.html", {"request": request})


@app.get("/session/{session_id}", response_class=HTMLResponse)
async def session_page(request: Request, session_id: str):
    async with session_lock:
        session = content_sessions.get(session_id)

    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    if not session["is_active"]:
        raise HTTPException(status_code=410, detail="Session has ended")

    return templates.TemplateResponse("session_viewer.html", {
        "request": request,
        "session_id": session_id,
        "session_name": session["session_name"],
        "teacher_name": session["teacher_name"],
        "content_url": session["content_url"],
        "content_type": session["content_type"],
        "embed_info": session["embed_info"],
    })


# -- Student Analytics Endpoints --

@app.post("/api/student/join")
@limiter.limit("30/minute")
async def student_join(request: Request, data: StudentJoinRequest):
    student_id = data.student_id
    student_name = data.name or f"Student {student_id}"
    session_id = data.session_id

    guid = f"{student_id}_{uuid.uuid4()}"
    start_time = datetime.now().isoformat()

    async with data_lock:
        students_data[student_id] = {
            'student_id': student_id,
            'name': student_name,
            'guid': guid,
            'session_id': session_id,
            'joined_at': start_time,
            'attention_score': 0,
            'emotion': 'neutral',
            'engagement_level': 'low',
            'face_present_ratio': 0.0,
            'gaze_on_screen_ratio': 0.0,
            'blink_rate': 0,
            'last_update': start_time,
            'total_updates': 0,
            'attention_sum': 0
        }

    if session_id:
        async with session_lock:
            session = content_sessions.get(session_id)
            if session:
                session["student_count"] = session.get("student_count", 0) + 1
                if student_id not in session.get("students", []):
                    session.setdefault("students", []).append(student_id)

    try:
        api_payload = [{
            "strGuid": guid,
            "intStudentId": int(student_id) if str(student_id).isdigit() else 0,
            "intSessionId": 0
        }]

        await http_client.post(
            f"{API_BASE}/InsertStudentSessionStart",
            json=api_payload,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        print(f"API warning (non-blocking): {e}")

    return {
        "status": "logged",
        "guid": guid,
        "message": f"Student {student_name} joined",
    }


@app.post("/api/student/update")
@limiter.limit("100/minute")
async def student_update(request: Request, data: StudentUpdateRequest):
    student_id = data.student_id

    if not student_id:
        raise HTTPException(status_code=400, detail="student_id required")

    async with data_lock:
        if student_id not in students_data:
            raise HTTPException(status_code=404, detail="Student not in session")

        student = students_data[student_id]

        student['attention_score'] = data.attention_score
        student['emotion'] = data.emotion
        student['engagement_level'] = data.engagement_level
        student['face_present_ratio'] = data.face_present_ratio
        student['gaze_on_screen_ratio'] = data.gaze_on_screen_ratio
        student['blink_rate'] = data.blink_rate
        student['last_update'] = datetime.now().isoformat()
        student['total_updates'] = student.get('total_updates', 0) + 1
        student['attention_sum'] = student.get('attention_sum', 0) + data.attention_score

        if data.session_id:
            student['session_id'] = data.session_id

        guid = student['guid']

    data_dict = data.model_dump()
    await add_to_timeline(student_id, data_dict)

    try:
        api_payload = [{
            "intAttentionScore": data.attention_score,
            "intBlinkRate": data.blink_rate,
            "decFacePresentRatio": data.face_present_ratio,
            "decGazeOnScreenpercentage": data.gaze_on_screen_ratio,
            "strEmotion": data.emotion,
            "strEngagement": data.engagement_level,
            "strGuid": guid,
            "intStudentId": int(student_id) if str(student_id).isdigit() else 0
        }]

        await http_client.post(
            f"{API_BASE}/InsertStudentSessionStartEventLogging",
            json=api_payload,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        print(f"API warning (non-blocking): {e}")

    return {"status": "success"}


@app.post("/api/student/leave")
async def student_leave(data: StudentLeaveRequest):
    student_id = data.student_id
    session_id = data.session_id

    async with data_lock:
        student_info = students_data.get(student_id)
        if not session_id and student_info:
            session_id = student_info.get('session_id')

        if student_id in students_data:
            del students_data[student_id]
        if student_id in student_timelines:
            del student_timelines[student_id]

    if session_id:
        async with session_lock:
            session = content_sessions.get(session_id)
            if session:
                session["student_count"] = max(0, session.get("student_count", 0) - 1)
                if student_id in session.get("students", []):
                    session["students"].remove(student_id)

    return {"status": "logged"}


# -- Analytics Endpoints --

@app.get("/api/analytics/overview")
@limiter.limit("60/minute")
async def analytics_overview(
    request: Request,
    session_id: Optional[str] = None,
    start_date: Optional[str] = None,
    start_time: Optional[str] = "00:00",
    end_date: Optional[str] = None,
    end_time: Optional[str] = "23:59"
):
    if not start_date or not end_date:
        today = datetime.now().date().strftime('%Y-%m-%d')
        start_date = end_date = today
        start_time, end_time = '00:00', '23:59'

    try:
        response = await http_client.post(
            f"{API_BASE}/GetMainTableByStartTimeRange",
            json={
                "dttStartTimeFrom": f"{start_date}T{start_time}:00",
                "dttStartTimeTo": f"{end_date}T{end_time}:59"
            },
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code not in [200, 201]:
            return empty_analytics_response()

        api_data = response.json()
        sessions = api_data.get('data', api_data) if isinstance(api_data, dict) else api_data

        if not isinstance(sessions, list) or not sessions:
            return empty_analytics_response()

        tasks = []
        for api_session in sessions:
            student_id = api_session.get('intStudentId')
            guid = api_session.get('strGuid', '')
            int_id = api_session.get('intId')

            if student_id and int_id:
                tasks.append(fetch_student_events(student_id, guid, int_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_students_data = [r for r in results if r and not isinstance(r, Exception)]

        # Filter by session_id if provided
        if session_id:
            async with data_lock:
                session_student_ids = set()
                for sid, sdata in students_data.items():
                    if sdata.get('session_id') == session_id:
                        session_student_ids.add(sid)
                        try:
                            session_student_ids.add(str(int(sid)))
                        except (ValueError, TypeError):
                            pass

            async with session_lock:
                cs = content_sessions.get(session_id)
                if cs:
                    for s in cs.get("students", []):
                        session_student_ids.add(s)

            if session_student_ids:
                all_students_data = [
                    s for s in all_students_data
                    if str(s.get('student_id', '')) in session_student_ids
                ]

        async with data_lock:
            for student in all_students_data:
                sid = str(student.get('student_id', ''))
                matched_name = None
                if sid in students_data:
                    matched_name = students_data[sid].get('name')
                else:
                    for stored_id, stored_data in students_data.items():
                        try:
                            if int(stored_id) == int(sid):
                                matched_name = stored_data.get('name')
                                break
                        except (ValueError, TypeError):
                            continue
                if matched_name:
                    student['name'] = matched_name

        return process_students_with_metrics(all_students_data)

    except Exception as e:
        print(f"Analytics API failed: {e}")
        return empty_analytics_response()


async def fetch_student_events(student_id: int, guid: str, int_id: int) -> Optional[dict]:
    try:
        response = await http_client.post(
            f"{API_BASE}/GetSessionEventLogging",
            json={
                "intId": int_id,
                "strGuid": guid,
                "intStudentId": student_id
            },
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code in [200, 201]:
            events_data = response.json()
            events = events_data.get('data', events_data) if isinstance(events_data, dict) else events_data

            if isinstance(events, list) and events:
                return aggregate_student_metrics(student_id, guid, events, int_id)
    except Exception as e:
        print(f"Error fetching events for student {student_id}: {e}")

    return None


def aggregate_student_metrics(student_id: int, guid: str, events: list, int_id: int) -> Optional[dict]:
    if not events:
        return None

    attention_scores: list = []
    emotion_counts: dict = {}
    engagement_counts: dict = {}
    face_ratios: list = []
    gaze_ratios: list = []

    for event in events:
        attention = event.get('intAttentionScore')
        if attention is not None:
            attention_scores.append(attention)

        emotion = event.get('strEmotion')
        if emotion:
            emotion_counts[emotion] = emotion_counts.get(emotion, 0) + 1

        engagement = event.get('strEngagement')
        if engagement:
            engagement_counts[engagement] = engagement_counts.get(engagement, 0) + 1

        face_ratio = event.get('decFacePresentRatio')
        if face_ratio is not None:
            face_ratios.append(face_ratio)

        gaze_ratio = event.get('decGazeOnScreenpercentage')
        if gaze_ratio is not None:
            gaze_ratios.append(gaze_ratio)

    avg_attention = sum(attention_scores) / len(attention_scores) if attention_scores else 0
    avg_face_ratio = sum(face_ratios) / len(face_ratios) if face_ratios else 0
    avg_gaze_ratio = sum(gaze_ratios) / len(gaze_ratios) if gaze_ratios else 0

    dominant_engagement = 'low'
    if engagement_counts:
        dominant_engagement = max(engagement_counts.items(), key=lambda x: x[1])[0].lower()

    return {
        'student_id': student_id,
        'name': f"Student {student_id}",
        'avg_attention': round(avg_attention, 1),
        'dominant_engagement': dominant_engagement,
        'total_metrics': len(events),
        'emotion_distribution': emotion_counts,
        'guid': guid,
        'face_present_ratio': avg_face_ratio,
        'gaze_on_screen_ratio': avg_gaze_ratio,
        'int_id': int_id
    }


def process_students_with_metrics(students_with_metrics: list) -> JSONResponse:
    if not students_with_metrics:
        return empty_analytics_response()

    emotions: dict = {}
    engagement_counts = {'high': 0, 'medium': 0, 'low': 0}
    total_attention = 0

    for student in students_with_metrics:
        total_attention += student['avg_attention']

        for emotion, count in student['emotion_distribution'].items():
            emotions[emotion] = emotions.get(emotion, 0) + count

        engagement = student['dominant_engagement']
        if engagement in engagement_counts:
            engagement_counts[engagement] += 1
        else:
            engagement_counts['low'] += 1

    avg_attention = total_attention / len(students_with_metrics)

    return JSONResponse({
        "status": "success",
        "data": {
            'overview': {
                'total_students': len(students_with_metrics),
                'avg_attention': round(avg_attention, 2),
                'high_engagement': engagement_counts['high'],
                'medium_engagement': engagement_counts['medium'],
                'low_engagement': engagement_counts['low'],
                'total_metrics_collected': sum(s['total_metrics'] for s in students_with_metrics)
            },
            'emotion_distribution': emotions,
            'students': students_with_metrics
        },
        "timestamp": datetime.now().isoformat()
    })


def empty_analytics_response() -> JSONResponse:
    return JSONResponse({
        "status": "success",
        "data": {
            'overview': {
                'total_students': 0,
                'avg_attention': 0,
                'high_engagement': 0,
                'medium_engagement': 0,
                'low_engagement': 0,
                'total_metrics_collected': 0
            },
            'emotion_distribution': {},
            'students': []
        },
        "timestamp": datetime.now().isoformat()
    })


@app.get("/api/analytics/student/{student_id}")
async def student_analytics(
    student_id: str,
    guid: Optional[str] = None,
    intId: Optional[str] = None
):
    if not guid:
        async with data_lock:
            if student_id in students_data:
                guid = students_data[student_id].get('guid', '')

    try:
        api_payload: dict = {
            "strGuid": guid or "",
            "intStudentId": int(student_id) if str(student_id).isdigit() else 0
        }

        if intId:
            api_payload["intId"] = int(intId) if str(intId).isdigit() else 0

        response = await http_client.post(
            f"{API_BASE}/GetSessionEventLogging",
            json=api_payload,
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code in [200, 201]:
            api_data = response.json()
            events = api_data.get('data', api_data) if isinstance(api_data, dict) else api_data

            if isinstance(events, list):
                return process_student_events(student_id, events)

        raise HTTPException(status_code=404, detail="Student not found")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Student API failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data")


def process_student_events(student_id: str, events: list) -> JSONResponse:
    if not events:
        raise HTTPException(status_code=404, detail="No data found")

    timeline: list = []
    attention_scores: list = []
    emotions: dict = {}
    engagements: dict = {}

    for event in events:
        attention = event.get('intAttentionScore', 0) or 0
        emotion = event.get('strEmotion', 'neutral') or 'neutral'
        engagement = (event.get('strEngagement', 'low') or 'low').lower()

        timeline.append({
            'timestamp': event.get('dttTimestamp') or datetime.now().isoformat(),
            'attention': attention,
            'emotion': emotion,
            'engagement': engagement
        })

        attention_scores.append(attention)
        emotions[emotion] = emotions.get(emotion, 0) + 1
        engagements[engagement] = engagements.get(engagement, 0) + 1

    if not attention_scores:
        attention_scores = [0]

    avg_attention = sum(attention_scores) / len(attention_scores)
    dominant_engagement = max(engagements, key=engagements.get) if engagements else 'low'

    return JSONResponse({
        "status": "success",
        "data": {
            'student_id': student_id,
            'name': f"Student {student_id}",
            'avg_attention': round(avg_attention, 2),
            'dominant_engagement': dominant_engagement,
            'total_metrics': len(timeline),
            'emotion_distribution': emotions,
            'session_start': timeline[0]['timestamp'] if timeline else None,
            'guid': events[0].get('strGuid', '') if events else '',
            'performance_summary': {
                'avg_attention': round(avg_attention, 2),
                'min_attention': min(attention_scores),
                'max_attention': max(attention_scores),
                'current_attention': attention_scores[-1] if attention_scores else 0,
                'total_sessions': len(timeline)
            },
            'engagement_distribution': engagements,
            'timeline': timeline[-20:]
        },
        "timestamp": datetime.now().isoformat()
    })


@app.get("/api/students/active")
async def active_students(session_id: Optional[str] = None):
    await cleanup_inactive_students()

    async with data_lock:
        if session_id:
            students = [
                s for s in students_data.values()
                if s.get('session_id') == session_id
            ]
        else:
            students = list(students_data.values())

    return {
        "timestamp": datetime.now().isoformat(),
        "active_count": len(students),
        "students": students
    }


@app.get("/api/realtime/alerts")
async def realtime_alerts(session_id: Optional[str] = None):
    alerts: list = []

    async with data_lock:
        for student_id, student in students_data.items():
            if session_id and student.get('session_id') != session_id:
                continue

            if student.get('attention_score', 0) < 30:
                alerts.append({
                    'type': 'low_attention',
                    'student_id': student_id,
                    'name': student.get('name', f'Student {student_id}'),
                    'attention': student.get('attention_score', 0),
                    'message': f"{student.get('name')} has very low attention ({student.get('attention_score', 0)}%)"
                })

            if student.get('engagement_level') == 'low':
                alerts.append({
                    'type': 'low_engagement',
                    'student_id': student_id,
                    'name': student.get('name', f'Student {student_id}'),
                    'engagement': student.get('engagement_level'),
                    'message': f"{student.get('name')} shows low engagement"
                })

    return {
        "status": "success",
        "alerts": alerts,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/api/admin/clear")
async def admin_clear():
    async with data_lock:
        students_data.clear()
        student_timelines.clear()
    async with session_lock:
        content_sessions.clear()
    return {"status": "cleared"}

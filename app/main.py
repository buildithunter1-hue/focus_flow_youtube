"""
Focus Tracker Backend - Enhanced Version
Production-ready for 500+ concurrent students
Fixes calculation bugs and adds mood tracking features
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
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
import os

from app.streaming import router as streaming_router

API_BASE= "https://davisEmailconfigureApi.softsolanalytics.com/API/User"

limiter = Limiter(key_func=get_remote_address)

students_data: dict = {}
student_timelines: dict = {}
mood_history: dict = {}
focus_sessions: dict = {}
data_lock = asyncio.Lock()

http_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=200)
    )
    print("=" * 60)
    print("FOCUS TRACKER - Enhanced Production Version")
    print("=" * 60)
    print("Features:")
    print("  - Fixed calculation bugs (includes zero scores)")
    print("  - Async API calls with connection pooling")
    print("  - Rate limiting (100 req/min per IP)")
    print("  - Mood history and trends")
    print("  - Focus session management")
    print("  - Scalable for 500+ students")
    print("=" * 60)
    yield
    await http_client.aclose()


app = FastAPI(title="Focus Tracker API", version="2.0.0", lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(streaming_router)

# Disable CORS. Do not remove this for full-stack development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


class StudentJoinRequest(BaseModel):
    student_id: str
    name: Optional[str] = None


class StudentUpdateRequest(BaseModel):
    student_id: str
    name: Optional[str] = None
    guid: Optional[str] = None
    attention_score: int = 0
    emotion: str = "neutral"
    engagement_level: str = "low"
    face_present_ratio: float = 0.0
    gaze_on_screen_ratio: float = 0.0
    blink_rate: int = 0
    face_detected_count: int = 0
    total_frame_count: int = 0


class StudentLeaveRequest(BaseModel):
    student_id: str
    name: Optional[str] = None


class FocusSessionRequest(BaseModel):
    student_id: str
    action: str
    duration_minutes: Optional[int] = 25


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
            'gaze_on_screen_ratio': data.get('gaze_on_screen_ratio', 0.0)
        })
        
        if len(student_timelines[student_id]) > 200:
            student_timelines[student_id] = student_timelines[student_id][-200:]


async def add_to_mood_history(student_id: str, emotion: str, attention: int):
    async with data_lock:
        if student_id not in mood_history:
            mood_history[student_id] = []
        
        mood_history[student_id].append({
            'timestamp': datetime.now().isoformat(),
            'emotion': emotion,
            'attention': attention
        })
        
        if len(mood_history[student_id]) > 500:
            mood_history[student_id] = mood_history[student_id][-500:]


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("student.html", {"request": request})


@app.get("/student", response_class=HTMLResponse)
async def student_page(request: Request):
    return templates.TemplateResponse("student.html", {"request": request})


@app.get("/teacher", response_class=HTMLResponse)
async def teacher_page(request: Request):
    return templates.TemplateResponse("teacher_analytics.html", {"request": request})


@app.get("/stream/broadcast", response_class=HTMLResponse)
async def teacher_stream_page(request: Request):
    return templates.TemplateResponse("teacher_stream.html", {"request": request})


@app.get("/stream/watch/{room_id}", response_class=HTMLResponse)
async def student_stream_page(request: Request, room_id: str):
    return templates.TemplateResponse("student_stream.html", {"request": request})


@app.get("/stream/watch", response_class=HTMLResponse)
async def student_stream_browse(request: Request):
    return templates.TemplateResponse("student_stream.html", {"request": request})


@app.post("/api/student/join")
@limiter.limit("30/minute")
async def student_join(request: Request, data: StudentJoinRequest):
    student_id = data.student_id
    student_name = data.name or f"Student {student_id}"
    
    guid = f"{student_id}_{uuid.uuid4()}"
    start_time = datetime.now().isoformat()
    
    async with data_lock:
        students_data[student_id] = {
            'student_id': student_id,
            'name': student_name,
            'guid': guid,
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
        
        focus_sessions[student_id] = {
            'active': True,
            'started_at': start_time,
            'breaks_taken': 0,
            'total_focus_time': 0,
            'last_break': None
        }
    
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
        "session_info": focus_sessions.get(student_id, {})
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
        
        guid = student['guid']
    
    await asyncio.gather(
        add_to_timeline(student_id, data.model_dump()),
        add_to_mood_history(student_id, data.emotion, data.attention_score)
    )
    
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
    
    break_suggestion = None
    async with data_lock:
        session = focus_sessions.get(student_id, {})
        if session.get('active'):
            started = datetime.fromisoformat(session['started_at'])
            elapsed = (datetime.now() - started).total_seconds() / 60
            last_break = session.get('last_break')
            
            if last_break:
                since_break = (datetime.now() - datetime.fromisoformat(last_break)).total_seconds() / 60
            else:
                since_break = elapsed
            
            if since_break >= 25:
                break_suggestion = {
                    'type': 'break_reminder',
                    'message': 'You have been focusing for 25+ minutes. Consider a 5-minute break!',
                    'focus_time': round(since_break, 1)
                }
    
    return {
        "status": "success",
        "break_suggestion": break_suggestion
    }


@app.post("/api/student/leave")
async def student_leave(data: StudentLeaveRequest):
    student_id = data.student_id
    
    async with data_lock:
        if student_id in students_data:
            del students_data[student_id]
        if student_id in student_timelines:
            del student_timelines[student_id]
        if student_id in focus_sessions:
            del focus_sessions[student_id]
    
    return {"status": "logged"}


@app.post("/api/focus/session")
async def manage_focus_session(data: FocusSessionRequest):
    student_id = data.student_id
    action = data.action
    
    async with data_lock:
        if student_id not in focus_sessions:
            focus_sessions[student_id] = {
                'active': False,
                'started_at': None,
                'breaks_taken': 0,
                'total_focus_time': 0,
                'last_break': None
            }
        
        session = focus_sessions[student_id]
        
        if action == 'start':
            session['active'] = True
            session['started_at'] = datetime.now().isoformat()
            session['last_break'] = None
            return {"status": "started", "session": session}
        
        elif action == 'pause':
            if session['active'] and session['started_at']:
                started = datetime.fromisoformat(session['started_at'])
                elapsed = (datetime.now() - started).total_seconds() / 60
                session['total_focus_time'] += elapsed
            session['active'] = False
            return {"status": "paused", "session": session}
        
        elif action == 'break':
            session['breaks_taken'] += 1
            session['last_break'] = datetime.now().isoformat()
            return {"status": "break_started", "session": session}
        
        elif action == 'resume':
            session['active'] = True
            session['started_at'] = datetime.now().isoformat()
            return {"status": "resumed", "session": session}
        
        elif action == 'end':
            if session['active'] and session['started_at']:
                started = datetime.fromisoformat(session['started_at'])
                elapsed = (datetime.now() - started).total_seconds() / 60
                session['total_focus_time'] += elapsed
            
            summary = {
                'total_focus_time': round(session['total_focus_time'], 1),
                'breaks_taken': session['breaks_taken']
            }
            
            session['active'] = False
            session['started_at'] = None
            session['total_focus_time'] = 0
            session['breaks_taken'] = 0
            
            return {"status": "ended", "summary": summary}
    
    return {"status": "unknown_action"}


@app.get("/api/mood/history/{student_id}")
async def get_mood_history(student_id: str, limit: int = 50):
    async with data_lock:
        history = mood_history.get(student_id, [])
    
    recent = history[-limit:] if len(history) > limit else history
    
    if not recent:
        return {
            "status": "success",
            "data": {
                "history": [],
                "trends": {},
                "summary": {}
            }
        }
    
    emotion_counts = {}
    attention_values = []
    
    for entry in recent:
        emotion = entry.get('emotion', 'neutral')
        emotion_counts[emotion] = emotion_counts.get(emotion, 0) + 1
        attention_values.append(entry.get('attention', 0))
    
    avg_attention = sum(attention_values) / len(attention_values) if attention_values else 0
    dominant_emotion = max(emotion_counts.items(), key=lambda x: x[1])[0] if emotion_counts else 'neutral'
    
    trend = "stable"
    if len(attention_values) >= 10:
        first_half = sum(attention_values[:len(attention_values)//2]) / (len(attention_values)//2)
        second_half = sum(attention_values[len(attention_values)//2:]) / (len(attention_values) - len(attention_values)//2)
        if second_half > first_half + 5:
            trend = "improving"
        elif second_half < first_half - 5:
            trend = "declining"
    
    return {
        "status": "success",
        "data": {
            "history": recent,
            "trends": {
                "attention_trend": trend,
                "dominant_emotion": dominant_emotion,
                "emotion_distribution": emotion_counts
            },
            "summary": {
                "avg_attention": round(avg_attention, 1),
                "total_entries": len(recent),
                "min_attention": min(attention_values) if attention_values else 0,
                "max_attention": max(attention_values) if attention_values else 0
            }
        }
    }


@app.get("/api/analytics/overview")
@limiter.limit("60/minute")
async def analytics_overview(
    request: Request,
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
        for session in sessions:
            student_id = session.get('intStudentId')
            guid = session.get('strGuid', '')
            int_id = session.get('intId')
            
            if student_id and int_id:
                tasks.append(fetch_student_events(student_id, guid, int_id))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_students_data = [r for r in results if r and not isinstance(r, Exception)]
        
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
    
    attention_scores = []
    emotion_counts = {}
    engagement_counts = {}
    face_ratios = []
    gaze_ratios = []
    
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
    
    emotions = {}
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
        api_payload = {
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
    
    timeline = []
    attention_scores = []
    emotions = {}
    engagements = {}
    
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
async def active_students():
    await cleanup_inactive_students()
    
    async with data_lock:
        students = list(students_data.values())
    
    return {
        "timestamp": datetime.now().isoformat(),
        "active_count": len(students),
        "students": students
    }


@app.get("/api/realtime/alerts")
async def realtime_alerts():
    alerts = []
    
    async with data_lock:
        for student_id, student in students_data.items():
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
        mood_history.clear()
        focus_sessions.clear()
    return {"status": "cleared"}

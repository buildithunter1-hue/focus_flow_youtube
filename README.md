# Focus Flow - YouTube Session Analytics

A FastAPI application for teachers to create video sessions and track student focus/engagement in real time using webcam-based face analytics.

## Features

- **Teacher Dashboard** — Create sessions with YouTube/Vimeo/HLS/MP4 content, manage active sessions, share links with students
- **Student Session Viewer** — Watch embedded video while webcam tracks attention using MediaPipe FaceMesh + face-api.js
- **Real-time Analytics** — Attention scoring, emotion detection, gaze tracking, Eye Aspect Ratio (EAR), head pose estimation
- **Teacher Analytics Dashboard** — Live overview of all students with charts, alerts, filters, and per-student drill-down

## Prerequisites

- Python 3.12+
- [Poetry](https://python-poetry.org/docs/#installation)

## Setup & Run

### 1. Install dependencies

```bash
poetry install
```

### 2. Set teacher credentials

```bash
export TEACHER_CREDENTIALS="admin:yourpassword"
```

For multiple teachers:

```bash
export TEACHER_CREDENTIALS="admin:pass123,teacher2:pass456"
```

### 3. Start the server

```bash
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The app will be available at `http://localhost:8000`

> **Note:** Use `localhost` instead of `127.0.0.1` — YouTube embeds require a proper origin and reject raw IP addresses.

## Pages & Links

| Page | URL | Description |
|------|-----|-------------|
| Teacher Dashboard | `http://localhost:8000/` | Login, create sessions, manage active sessions |
| Teacher Analytics | `http://localhost:8000/teacher/analytics` | Live student metrics, charts, alerts, per-student details |
| Student Session | `http://localhost:8000/session/{session_id}` | Student view — video + webcam focus tracker (share this link) |
| Health Check | `http://localhost:8000/healthz` | Server health status |

## How to Use

1. Open `http://localhost:8000/` and log in with your teacher credentials
2. Enter a video URL (YouTube, Vimeo, MP4, or HLS) and click **Create Session**
3. Copy the session share link and send it to students
4. Students open the link, enter their name, allow webcam access, and start watching
5. Go to `http://localhost:8000/teacher/analytics` to see live student focus data

## API Endpoints

### Authentication
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/login` | Teacher login (returns auth token) |
| POST | `/api/auth/verify` | Verify auth token |

### Sessions
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/session/create` | Create a new session (requires auth) |
| GET | `/api/sessions/active` | List all active sessions |
| GET | `/api/session/{session_id}/info` | Get session details |
| POST | `/api/session/{session_id}/close` | Close a session (requires auth) |

### Student Tracking
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/student/join` | Student joins a session |
| POST | `/api/student/update` | Send focus metrics update |
| POST | `/api/student/leave` | Student leaves session |

### Analytics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/analytics/overview` | Overview of all student metrics |
| GET | `/api/analytics/student/{student_id}` | Per-student analytics |
| GET | `/api/students/active` | List currently active students |
| GET | `/api/realtime/alerts` | Real-time low-attention alerts |

### Admin
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/admin/clear` | Clear all in-memory data |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEACHER_CREDENTIALS` | Yes | Teacher login credentials in `user:pass` format (comma-separated for multiple) |

## Tech Stack

- **Backend:** FastAPI, Jinja2, httpx, slowapi (rate limiting)
- **Face Detection:** MediaPipe FaceMesh (landmarks), face-api.js (emotion detection)
- **Charts:** Chart.js
- **Video:** YouTube/Vimeo iframe embeds, HLS.js for live streams

## Notes

- Webcam requires **HTTPS** (or `localhost`) — it will not work over plain HTTP on non-localhost domains
- Data is stored **in-memory** — restarting the server clears all session/student data
- The student update endpoint auto-registers students if they aren't found (handles server restart gracefully)

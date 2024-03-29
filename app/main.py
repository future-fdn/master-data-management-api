from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.config import get_settings
from app.routers import auth, files, tasks, users

settings = get_settings()

app = FastAPI(
    title="master data management api",
    version="1.0.0",
    description="https://github.com/future-fdn/master-data-management-api",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origin,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"],
)

v1 = APIRouter(prefix="/api/v1", tags=["v1"])
v1.include_router(auth.router, tags=["auth"])
v1.include_router(files.router, tags=["files"])
v1.include_router(users.router, tags=["users"])
v1.include_router(tasks.router, tags=["users"])
app.include_router(v1)


@app.get("/")
async def root():
    return {"message": "API is working."}

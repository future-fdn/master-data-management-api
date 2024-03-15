from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.auth import routes as auth
from app.config import settings
from app.files import routes as files
from app.tasks import routes as tasks
from app.users import routes as users

app = FastAPI(
    title="master data management api",
    version="1.0.0",
    description="https://github.com/future-fdn/master-data-management-api",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.ALLOWED_HOSTS,
)

v1 = APIRouter(prefix="/api/v1", tags=["v1"])

v1.include_router(users.router, tags=["user"])
v1.include_router(files.router, tags=["files"])
v1.include_router(auth.router, tags=["auth"])
v1.include_router(tasks.router, tags=["tasks"])

app.include_router(v1)


@app.get("/")
async def root():
    return {"message": "API is working."}

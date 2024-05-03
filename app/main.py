import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from sqlalchemy import func, insert, or_, select

from app.config import get_settings
from app.db import get_async_session
from app.models.file import DataQuality, File
from app.routers import auth, files, tasks, users

settings = get_settings()
logger = logging.getLogger("scheduler")


class SchedulerService:
    @staticmethod
    async def check_dq():
        logger.info("running data quality sync")
        async with get_async_session() as session:
            this_month = await session.scalar(
                select(DataQuality).where(
                    DataQuality.date == datetime.now().replace(day=1).date()
                )
            )

            if not this_month:
                unique = await session.scalar(
                    select(func.sum(File.unique))
                    .select_from(File)
                    .where(or_(File.type == "QUERY", File.type == "MASTER"))
                )

                valid = await session.scalar(
                    select(func.sum(File.valid))
                    .select_from(File)
                    .where(or_(File.type == "QUERY", File.type == "MASTER"))
                )

                total = await session.scalar(
                    select(func.sum(File.total))
                    .select_from(File)
                    .where(or_(File.type == "QUERY", File.type == "MASTER"))
                )

                master = await session.scalar(
                    select(func.sum(File.total))
                    .select_from(File)
                    .where(File.type == "MASTER")
                )

                query = await session.scalar(
                    select(func.sum(File.total))
                    .select_from(File)
                    .where(File.type == "QUERY")
                )

                overall_uniqueness = unique / total
                overall_completeness = valid / total

                await session.scalar(
                    insert(DataQuality)
                    .values(
                        date=datetime.now().replace(day=1).date(),
                        overall_completeness=overall_completeness,
                        overall_uniqueness=overall_uniqueness,
                        total_query_records=query,
                        total_master_records=master,
                    )
                    .returning(DataQuality)
                )

                await session.commit()

    def start(self):
        logger.info("Starting scheduler service.")
        self.queue = asyncio.Queue()
        self.sch = AsyncIOScheduler()
        self.sch.start()
        self.sch.add_job(
            SchedulerService.check_dq,
            "interval",
            seconds=60 * 60 * 24,
            # Using max_instances=1 guarantees that only one job
            # runs at the same time (in this event loop).
            max_instances=1,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await SchedulerService.check_dq()

    sch_srv = SchedulerService()
    sch_srv.start()

    yield


app = FastAPI(
    title="master data management api",
    version="1.0.0",
    description="https://github.com/future-fdn/master-data-management-api",
    lifespan=lifespan,
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

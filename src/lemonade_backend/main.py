from contextlib import asynccontextmanager

from fastapi import FastAPI

from lemonade_backend.api.routes import audit, jobs, operations, pipelines
from lemonade_backend.catalog.registry import register_all_executors
from lemonade_backend.config import get_settings
from lemonade_backend.db.session import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    register_all_executors()
    yield


app = FastAPI(
    title=get_settings().app_name,
    version="0.1.0",
    description="Lemonade-compatible Data Science API on PySpark",
    lifespan=lifespan,
)

app.include_router(operations.router)
app.include_router(jobs.router)
app.include_router(pipelines.router)
app.include_router(audit.router)


@app.get("/health")
async def health():
    return {"status": "ok"}

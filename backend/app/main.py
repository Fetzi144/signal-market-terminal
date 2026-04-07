"""Signal Market Terminal — FastAPI application."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.api import signals, markets, health
from app.jobs.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Signal Market Terminal")
    start_scheduler()
    yield
    logger.info("Shutting down Signal Market Terminal")
    stop_scheduler()


app = FastAPI(
    title="Signal Market Terminal",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(signals.router)
app.include_router(markets.router)
app.include_router(health.router)


@app.get("/")
async def root():
    return {"name": "Signal Market Terminal", "version": "0.1.0"}

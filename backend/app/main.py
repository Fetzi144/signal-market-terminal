"""Signal Market Terminal — FastAPI application."""
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.api import alerts, analytics, health, markets, signals, sse
from app.config import settings
from app.jobs.scheduler import start_scheduler, stop_scheduler

# Structured JSON logging in production, plain text in development
log_level = getattr(logging, settings.log_level)
if settings.log_format == "json":
    from pythonjsonlogger.json import JsonFormatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=log_level, handlers=[handler])
else:
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address, default_limits=[settings.api_rate_limit])


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

app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def _rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": f"Rate limit exceeded: {exc.detail}"},
    )


# Optional API key auth middleware
if settings.api_key:
    @app.middleware("http")
    async def _api_key_middleware(request: Request, call_next):
        # Skip auth for health check and root
        if request.url.path in ("/", "/api/v1/health"):
            return await call_next(request)
        key = request.headers.get("x-api-key")
        if key != settings.api_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
        return await call_next(request)


# Configurable CORS
cors_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(signals.router)
app.include_router(markets.router)
app.include_router(health.router)
app.include_router(alerts.router)
app.include_router(sse.router)
app.include_router(analytics.router)

# Prometheus auto-instrumentation — exposes /metrics
Instrumentator().instrument(app).expose(app)


@app.get("/")
async def root():
    return {"name": "Signal Market Terminal", "version": "0.2.0"}

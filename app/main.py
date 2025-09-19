from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv
from app.config import settings

from app.api.routes import validation_router, dashboard_router
from app.api.sheet_router import router as sheet_router
from app.api.sheet_router import ensure_sheet_cache_on_start
from app.database.connection import init_database

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="Sector Guards",
    description="A dashboard for data validation with automated anomaly detection and email notifications",
    version="1.0.0"
)

# Resolve frontend URL and CORS origins from centralized settings (robust parsing)
DEFAULT_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# Production frontend URL
PRODUCTION_FRONTEND = "https://sectors-guard.vercel.app"

FRONTEND_URL = os.getenv("FRONTEND_URL", DEFAULT_ORIGINS[0])

# Use settings.cors_origins which handles JSON-like strings and dedupes with defaults
ALLOW_ORIGINS = list(settings.cors_origins)

# Ensure important origins are present without duplicates
if PRODUCTION_FRONTEND not in ALLOW_ORIGINS:
    ALLOW_ORIGINS.append(PRODUCTION_FRONTEND)
if FRONTEND_URL and FRONTEND_URL not in ALLOW_ORIGINS:
    ALLOW_ORIGINS.insert(0, FRONTEND_URL)

# Configure CORS with comprehensive settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
    allow_headers=[
        "Accept",
        "Accept-Language",
        "Content-Language",
        "Content-Type",
        "Authorization",
        "X-Requested-With",
        "X-Custom-Header",
        "Cache-Control",
        "Pragma",
        "Expires"
    ],
    expose_headers=["*"],
    max_age=3600,
)

# Include routers
app.include_router(validation_router, prefix="/api/validation", tags=["validation"])
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(sheet_router)

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    try:
        print("🚀 Starting Sector Guards...")
        success = init_database()
        if success:
            print("✅ Database connection initialized successfully")
        else:
            print("⚠️  Database connection failed, but app will continue")
    except Exception as e:
        print(f"⚠️  Database initialization error: {e}")
        print("📝 App will start without database connection")

    # Debug: print frontend URL / CORS origins on startup
    try:
        print(f"🔧 FRONTEND_URL = {FRONTEND_URL}")
        print(f"🔧 CORS_ORIGINS = {ALLOW_ORIGINS}")
    except Exception as e:
        print(f"⚠️  Could not read settings: {e}")

    # Optionally warm up the sheet cache on startup
    try:
        await ensure_sheet_cache_on_start()
    except Exception as e:
        print(f"⚠️  Sheet prefetch on startup failed: {e}")

@app.get("/")
async def root():
    return {"message": "Sectors Guard API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/debug-config")
async def debug_config():
    """Return configuration values useful for debugging environment and CORS."""
    try:
        return {
            "frontend_url": FRONTEND_URL,
            "cors_origins": ALLOW_ORIGINS,
            "env_frontend_url": os.getenv("FRONTEND_URL"),
            "env_cors_origins": os.getenv("CORS_ORIGINS"),
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.base import BaseHTTPMiddleware
import os
from dotenv import load_dotenv

from app.api.routes import validation_router, dashboard_router
from app.database.connection import init_database

# Load environment variables
load_dotenv()

class ExplicitCORSMiddleware(BaseHTTPMiddleware):
    """Explicit CORS middleware to ensure headers are always set"""
    
    def __init__(self, app, allowed_origins):
        super().__init__(app)
        self.allowed_origins = allowed_origins
    
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        
        # Get origin from request
        origin = request.headers.get("origin")
        
        # Set CORS headers explicitly
        if origin in self.allowed_origins or "*" in self.allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
        else:
            response.headers["Access-Control-Allow-Origin"] = "*"
            
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Max-Age"] = "3600"
        
        return response

# Create FastAPI app
app = FastAPI(
    title="Sector Guards",
    description="A dashboard for data validation with automated anomaly detection and email notifications",
    version="1.0.0"
)

# Resolve frontend URL and CORS origins from environment
DEFAULT_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# Production frontend URL
PRODUCTION_FRONTEND = "https://sectors-guard.vercel.app"

FRONTEND_URL = os.getenv("FRONTEND_URL", DEFAULT_ORIGINS[0])

if os.getenv("CORS_ORIGINS"):
    ALLOW_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
else:
    # Ensure production frontend is always included
    origins_set = set([FRONTEND_URL, PRODUCTION_FRONTEND] + DEFAULT_ORIGINS)
    ALLOW_ORIGINS = list(origins_set)

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

# Add explicit CORS middleware as backup
app.add_middleware(ExplicitCORSMiddleware, allowed_origins=ALLOW_ORIGINS + ["*"])

# Include routers
app.include_router(validation_router, prefix="/api/validation", tags=["validation"])
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])

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

@app.get("/")
async def root():
    return {"message": "Sectors Guard API", "version": "1.0.0"}

@app.options("/{path:path}")
async def options_handler(path: str):
    """Handle OPTIONS requests for CORS preflight"""
    return JSONResponse(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )

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
            "env_port": os.getenv("PORT"),
            "production_frontend": PRODUCTION_FRONTEND,
            "default_origins": DEFAULT_ORIGINS,
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/cors-test") 
async def cors_test():
    """Simple endpoint to test CORS functionality"""
    return JSONResponse(
        content={"message": "CORS test successful", "timestamp": "2025-09-15"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

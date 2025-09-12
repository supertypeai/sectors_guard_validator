from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv

from app.api.routes import validation_router, dashboard_router
from app.database.connection import init_database
from app.config import settings

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="Sector Guards",
    description="A dashboard for data validation with automated anomaly detection and email notifications",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.get("/")
async def root():
    return {"message": "Sectors Guard API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

"""
IDX Data Validation Backend Application
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv

from app.api.routes import validation_router, dashboard_router
from app.database.connection import init_database

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="IDX Data Validation Dashboard",
    description="A dashboard for data validation with automated anomaly detection and email notifications",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
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
    init_database()

@app.get("/")
async def root():
    return {"message": "IDX Data Validation Dashboard API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

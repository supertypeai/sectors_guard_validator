from fastapi import Depends, HTTPException, Header, status
import os
from typing import Optional


def verify_bearer_token(authorization: Optional[str] = Header(None)) -> None:
	"""Simple bearer token verification using BACKEND_API_TOKEN env var.

	Raise 401 if header missing/invalid, 403 if token mismatch.
	"""
	expected = os.getenv("BACKEND_API_TOKEN")
	if not expected:
		# If not configured, allow but warn via exception detail? We choose to deny for safety.
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="BACKEND_API_TOKEN not configured")

	if not authorization or not authorization.lower().startswith("bearer "):
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")

	token = authorization.split(None, 1)[1].strip()
	if token != expected:
		raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

	# return None on success; can be extended to return user context
	return None


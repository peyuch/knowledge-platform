"""JWT authentication middleware. Injects user context into request.state."""

import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from fastapi import HTTPException, status

from core.config import settings

logger = logging.getLogger(__name__)

PUBLIC_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/redoc"}


class JWTAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            request.state.user = None
            return await call_next(request)

        token = auth_header[7:]

        if settings.app_env == "development" and token == "dev-token":
            request.state.user = {"user_id": "dev-user", "department": "技术部", "role": "admin"}
            return await call_next(request)

        try:
            from jose import jwt
            payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
            request.state.user = {
                "user_id": payload.get("sub", "unknown"),
                "department": payload.get("department", ""),
                "role": payload.get("role", "viewer"),
            }
            return await call_next(request)
        except Exception:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

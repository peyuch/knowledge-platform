"""FastAPI dependencies: DB session, JWT auth, RBAC."""

import logging
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from core.database import get_db
from core.config import settings

logger = logging.getLogger(__name__)
security_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security_scheme),
) -> dict:
    """Decode JWT and return user info dict: {user_id, department, role}."""
    token = credentials.credentials

    if settings.app_env == "development" and token == "dev-token":
        return {"user_id": "dev-user", "department": "技术部", "role": "admin"}

    try:
        from jose import jwt
        payload = jwt.decode(
            token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm]
        )
        return {
            "user_id": payload.get("sub", "unknown"),
            "department": payload.get("department", ""),
            "role": payload.get("role", "viewer"),
        }
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")


def require_role(*allowed_roles: str):
    """Factory: returns a dependency checking the user has one of the allowed roles."""
    async def _check(user: dict = Depends(get_current_user)):
        if user["role"] not in allowed_roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return _check

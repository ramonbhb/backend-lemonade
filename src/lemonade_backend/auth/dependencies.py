from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from lemonade_backend.config import get_settings

security = HTTPBearer(auto_error=False)


@dataclass
class AuthContext:
    user_id: str
    tenant_id: str
    roles: list[str]


async def get_auth_context(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> AuthContext:
    settings = get_settings()
    if settings.auth_dev_mode:
        return AuthContext(user_id="dev-user", tenant_id="dev-tenant", roles=["admin"])

    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    from lemonade_backend.auth.keycloak import validate_token

    return await validate_token(credentials.credentials)


def require_role(role: str):
    async def checker(ctx: AuthContext = Depends(get_auth_context)) -> AuthContext:
        if role not in ctx.roles and "admin" not in ctx.roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role")
        return ctx

    return checker

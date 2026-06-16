from __future__ import annotations

import httpx
from jose import jwt

from lemonade_backend.auth.dependencies import AuthContext
from lemonade_backend.config import get_settings


async def validate_token(token: str) -> AuthContext:
    settings = get_settings()
    if not settings.keycloak_url:
        raise ValueError("KEYCLOAK_URL required when AUTH_DEV_MODE=false")

    jwks_url = (
        f"{settings.keycloak_url}/realms/{settings.keycloak_realm}"
        "/protocol/openid-connect/certs"
    )
    async with httpx.AsyncClient() as client:
        jwks = (await client.get(jwks_url)).json()

    unverified = jwt.get_unverified_header(token)
    key = next(k for k in jwks["keys"] if k["kid"] == unverified["kid"])
    payload = jwt.decode(token, key, algorithms=["RS256"], audience=settings.keycloak_client_id)

    tenant_id = payload.get("tenant_id") or payload.get("org_id") or "default"
    user_id = payload.get("sub", "unknown")
    roles = payload.get("realm_access", {}).get("roles", [])
    return AuthContext(user_id=user_id, tenant_id=tenant_id, roles=roles)

from fastapi import APIRouter, Request, Response
import httpx
import os
from utils import should_use_new_service

router = APIRouter()

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")

def get_target_url(path: str) -> str:
    if "/api/movies" in path:
        base = MOVIES_SERVICE_URL if should_use_new_service() else MONOLITH_URL
    else:
        base = MONOLITH_URL
    return f"{base}{path}"

async def proxy_request(request: Request, path: str):
    full_url = get_target_url(path)

    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method=request.method,
                url=full_url,
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
                content=await request.body(),
                params=request.query_params
            )
        except httpx.RequestError as e:
            return Response(
                content=f"Proxy request failed: {str(e)}",
                status_code=502
            )

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={k: v for k, v in response.headers.items() if k.lower() != "content-encoding"}
    )


@router.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE"])
async def movies_proxy(request: Request):
    return await proxy_request(request, "/api/movies")

@router.api_route("/api/movies/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def movies_proxy_dynamic(request: Request, path: str):
    return await proxy_request(request, f"/api/movies/{path}")

@router.api_route("/api/users", methods=["GET", "POST", "PUT", "DELETE"])
async def users_proxy(request: Request):
    return await proxy_request(request, "/api/users")

@router.api_route("/api/users/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def users_proxy_dynamic(request: Request, path: str):
    return await proxy_request(request, f"/api/users/{path}")

@router.api_route("/api/payments", methods=["GET", "POST", "PUT", "DELETE"])
async def payments_proxy(request: Request):
    return await proxy_request(request, "/api/payments")

@router.api_route("/api/payments/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def payments_proxy_dynamic(request: Request, path: str):
    return await proxy_request(request, f"/api/payments/{path}")

@router.api_route("/api/subscriptions", methods=["GET", "POST", "PUT", "DELETE"])
async def subscriptions_proxy(request: Request):
    return await proxy_request(request, "/api/subscriptions")

@router.api_route("/api/subscriptions/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def subscriptions_proxy_dynamic(request: Request, path: str):
    return await proxy_request(request, f"/api/subscriptions/{path}")


@router.get("/health")
async def health_check():
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            monolith_resp = await client.get(f"{MONOLITH_URL}/health")
            movies_resp = await client.get(f"{MOVIES_SERVICE_URL}/api/movies/health")

        monolith_ok = monolith_resp.status_code == 200
        movies_ok = movies_resp.status_code == 200

        if monolith_ok and movies_ok:
            return {"status": "ok", "monolith": True, "movies": True}
        else:
            return Response(
                content=f'{{"status": "degraded", "monolith": {monolith_ok}, "movies": {movies_ok}}}',
                media_type="application/json",
                status_code=500
            )

    except Exception as e:
        return Response(
            content=f'{{"status": "error", "detail": "{str(e)}"}}',
            media_type="application/json",
            status_code=500
        )

from fastapi import APIRouter, Request, Response
import httpx
import os
from utils import should_use_new_service

router = APIRouter()

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")

@router.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE"])
async def movies_proxy(request: Request):
    url = MOVIES_SERVICE_URL if should_use_new_service() else MONOLITH_URL
    full_url = f"{url}/api/movies"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method=request.method,
                url=full_url,
                headers={k: v for k, v in request.headers.items()},
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
        headers=dict(response.headers)
    )

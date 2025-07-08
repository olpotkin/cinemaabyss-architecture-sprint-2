from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from kafka_client import produce_event, consume_events, producer
import json

app = FastAPI()

REQUIRED_FIELDS = {
    "user": ["user_id", "username", "action", "timestamp"],
    "movie": ["movie_id", "title", "action", "user_id"],
    "payment": ["payment_id", "user_id", "amount", "status", "timestamp", "method_type"]
}

@app.on_event("startup")
def on_startup():
    consume_events()
    print("[Startup] Kafka consumer started.")

@app.get("/api/events/health")
async def health_check():
    try:
        producer.partitions_for("user-events")
        return {"status": True}
    except Exception as e:
        print(f"[HealthCheck] Kafka unavailable: {e}")
        return JSONResponse(status_code=500, content={"status": False, "error": "Kafka unavailable"})

@app.post("/api/events/{event_type}")
async def publish_event(event_type: str, request: Request):
    try:
        raw_body = await request.body()

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as e:
            print(f"[Error] Invalid payload: {e}")
            print(f"[Debug] Raw body: {raw_body.decode('utf-8')}")
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid JSON", "details": str(e)}
            )

        required = REQUIRED_FIELDS.get(event_type, [])
        missing = [field for field in required if field not in payload or payload[field] in ("", None)]
        if missing:
            print(f"[Error] Missing fields for '{event_type}': {missing}")
            return JSONResponse(
                status_code=400,
                content={"error": "Missing required fields", "missing_fields": missing}
            )

        print(f"[API] Received event: {event_type} -> {payload}")
        produce_event(event_type, payload)

        return JSONResponse(
            status_code=201,
            content={"status": "success", "type": event_type}
        )

    except Exception as e:
        print(f"[Exception] Unexpected error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "details": str(e)}
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8082)

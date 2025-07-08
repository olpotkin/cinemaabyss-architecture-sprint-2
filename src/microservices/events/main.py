from fastapi import FastAPI
from kafka_client import produce_event, consume_events

app = FastAPI()

@app.on_event("startup")
def start_consumer():
    consume_events()

@app.post("/api/events/{event_type}")
def post_event(event_type: str, payload: dict):
    produce_event(event_type, payload)
    return {"status": "published", "type": event_type}

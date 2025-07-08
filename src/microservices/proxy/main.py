from fastapi import FastAPI
from router import router
import os
import uvicorn

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

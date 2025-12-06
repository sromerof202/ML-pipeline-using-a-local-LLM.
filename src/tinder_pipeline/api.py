import os

import redis
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, make_asgi_app

app = FastAPI()
metrics_app = make_asgi_app()
app.mount("/metrics/", metrics_app)

HITS = Counter("api_hits_total", "Total API calls")
r = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379, db=0, decode_responses=True)


@app.get("/user/{user_id}/risk")
def check_user(user_id: str):
    HITS.inc()
    data = r.hgetall(f"user_risk:{user_id}")
    if not data:
        raise HTTPException(status_code=404, detail="User unknown")
    return data

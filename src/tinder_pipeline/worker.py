import json
import os
import time

import httpx
import redis
from prometheus_client import Counter, Histogram, start_http_server

# --- Metrics ---
PROCESSED = Counter("emails_processed_total", "Total emails analyzed")
THREATS = Counter("threats_detected_total", "Total threats found")
LATENCY = Histogram("llm_processing_seconds", "Time taken by LLM")

# --- Config ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
# This connects to the service named "ollama" in docker-compose
OLLAMA_ADDR = os.getenv("OLLAMA_ADDR", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

r = redis.Redis(host=REDIS_HOST, port=6379, db=0)


def analyze(text: str) -> dict:
    """
    Sends text to Local Ollama.
    Forces JSON output using the 'format: json' parameter.
    """
    system_prompt = (
        "You are a Trust & Safety bot. "
        "Analyze the message for clear indicators of scam, harassment, "
        "or insider threat. "
        "Rules:\n"
        "1. If the text is just gibberish, broken English, or nonsense, "
        "output 'is_risky': false.\n"
        "2. Only flag as 'is_risky': true if there is explicit evidence of "
        "harm, theft, or violence.\n"
        "Output ONLY a JSON object with keys: 'is_risky' (boolean) "
        "and 'reason' (string)."
    )

    with LATENCY.time():
        try:
            payload = {
                "model": OLLAMA_MODEL,
                "prompt": text,
                "system": system_prompt,
                "stream": False,  # IMPORTANT: Get the full response at once
                "format": "json",  # IMPORTANT: Force valid JSON
            }

            # Timeout set to 60s because local CPU inference can be slow
            response = httpx.post(
                f"{OLLAMA_ADDR}/api/generate", json=payload, timeout=60.0
            )
            response.raise_for_status()

            # Ollama returns the result in the "response" key
            body = response.json()
            generated_text = body.get("response", "{}")

            return json.loads(generated_text)

        except Exception as e:
            print(f"Worker Error: {e}")
            return {"is_risky": False, "reason": "LLM_Error"}


def run():
    # Start Prometheus metrics
    start_http_server(8001)
    print(f"Worker connecting to Ollama at {OLLAMA_ADDR}...")

    # Wait for Ollama to wake up
    while True:
        try:
            httpx.get(f"{OLLAMA_ADDR}")
            print("Connected to Ollama!")
            break
        except Exception as e:
            print(f"Waiting for Ollama service... ({e})")
            time.sleep(5)

    print("Worker listening for tasks...")
    while True:
        # Blocking Pop from Redis
        # This waits forever until a message arrives
        _, msg = r.blpop("ml_task_queue")

        # --- CRASH PROTECTION START ---
        try:
            if not msg:
                continue  # Skip empty messages
            data = json.loads(msg)
        except json.JSONDecodeError as e:
            print(f"⚠️ [WARNING] skipping toxic message (bad JSON): {e}")
            continue  # Skip to next loop, DO NOT CRASH
        except Exception as e:
            print(f"⚠️ [WARNING] Unknown error parsing message: {e}")
            continue
        # --- CRASH PROTECTION END ---

        PROCESSED.inc()

        # Analyze
        result = analyze(data["content"])

        if result.get("is_risky"):
            THREATS.inc()
            print(f"🔴[RISK DETECTED] User {data['user_id']}: {result.get('reason')}")
        else:
            print(f"🟢 [SAFE] User {data['user_id']}: {result.get('reason')}")

        # Save to Feature Store
        r.hmset(
            f"user_risk:{data['user_id']}",
            {"risky": str(result.get("is_risky")), "reason": str(result.get("reason"))},
        )


if __name__ == "__main__":
    run()

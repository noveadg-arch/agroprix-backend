"""Startup script - reads PORT from environment for Railway/Render compatibility."""
import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"[AgroPrix] Starting on port {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)

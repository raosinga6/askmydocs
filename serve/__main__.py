"""Entry point: python -m serve"""
import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "serve.app:app",
        host=os.environ.get("ASKMYDOCS_HOST", "127.0.0.1"),
        port=int(os.environ.get("ASKMYDOCS_PORT", "8008")),
    )

# main.py
# Minimal entrypoint — creates FastAPI app and initializes services.
from fastapi import FastAPI
import services

app = FastAPI()

# Initialize services (register routes, background tasks, DB, WS, etc.)
services.init_app(app)

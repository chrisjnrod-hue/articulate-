# run_services.py
# Minimal entrypoint that wires the split services modules into a FastAPI app.

from fastapi import FastAPI
import services_core

app = FastAPI()
services_core.init_app(app)

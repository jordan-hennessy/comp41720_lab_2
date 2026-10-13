from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

reserved = {}  # document_id -> units

# --- Models ---


class QuotaRequest(BaseModel):
    tenant_id: str
    document_id: str
    units: int = 1


# --- Helpers ---


# --- Routes ---


@app.get("/quota/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/quota/reserve")
def reserve(req: QuotaRequest) -> dict:
    reserved[req.document_id] = req.units
    return {"status": "reserved"}


@app.post("/quota/release")
def release(req: QuotaRequest) -> dict:
    reserved.pop(req.document_id, None)
    return {"status": "released"}


@app.get("/quota/state")
def state() -> dict:
    return {"reserved_units": sum(reserved.values())}


# RUN 
# poetry run uvicorn lab_2.quota:app --port 8081
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import select
from database import get_db
from models import Dolar
from schemas import RangeRequest, RangeResponse, Dato
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os


app = FastAPI(title="Valores API")
# al crear el app...
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    # sirve el front
    return FileResponse(os.path.join("static", "index.html"))


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/valores/rango", response_model=RangeResponse)
def valores_por_rango(payload: RangeRequest, db: Session = Depends(get_db)):
    if payload.end <= payload.start:
        raise HTTPException(status_code=400, detail="end debe ser > start")

    stmt = (
        select(Dolar.fecha, Dolar.valor)
        .where(Dolar.fecha >= payload.start, Dolar.fecha <= payload.end)
        .order_by(Dolar.fecha.asc())
        .limit(payload.limit)
    )
    rows = db.execute(stmt).all()  # lista de tuplas (fecha, valor)

    items = [Dato(fecha=r[0], valor=float(r[1])) for r in rows]
    return RangeResponse(count=len(items), items=items)

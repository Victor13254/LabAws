from sqlalchemy import Column, DateTime, Numeric
from database import Base

class Dolar(Base):
    __tablename__ = "dolar"
    fecha = Column(DateTime, primary_key=True)         # UTC recomendado
    valor = Column(Numeric(12, 6), nullable=False)     # DECIMAL(12,6)

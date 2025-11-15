from sqlalchemy import Column, Integer, String, Boolean, DateTime, JSON
from sqlalchemy.sql import func
from app.database import Base


class Webhook(Base):
    __tablename__ = "webhooks"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, nullable=False)
    event_types = Column(JSON, nullable=False)  # Array of event type strings
    enabled = Column(Boolean, default=True, nullable=False, index=True)
    secret = Column(String, nullable=True)  # Optional HMAC secret
    headers = Column(JSON, nullable=True)  # Custom headers as key-value pairs
    timeout = Column(Integer, default=30, nullable=False)  # Request timeout in seconds
    retry_count = Column(Integer, default=3, nullable=False)  # Max retry attempts
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<Webhook(id={self.id}, url='{self.url}', enabled={self.enabled})>"


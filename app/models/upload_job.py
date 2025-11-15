from sqlalchemy import Column, Integer, String, Float, DateTime, Enum
from sqlalchemy.sql import func
import enum
from app.database import Base


class UploadStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class UploadJob(Base):
    __tablename__ = "upload_jobs"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, unique=True, nullable=False, index=True)  # Celery task ID
    status = Column(Enum(UploadStatus), default=UploadStatus.PENDING, nullable=False, index=True)
    progress = Column(Float, default=0.0, nullable=False)  # Progress percentage (0-100)
    error_message = Column(String, nullable=True)
    total_rows = Column(Integer, nullable=True)
    processed_rows = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<UploadJob(id={self.id}, task_id='{self.task_id}', status='{self.status}', progress={self.progress}%)>"


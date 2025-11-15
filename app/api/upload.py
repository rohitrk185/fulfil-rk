import os
import uuid
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Annotated
from app.database import get_db
from app.models.upload_job import UploadJob, UploadStatus
from app.tasks.csv_processor import process_csv_upload
from celery.result import AsyncResult
from celery_app import celery_app

router = APIRouter(prefix="/api/upload", tags=["upload"])

# Directory to store uploaded files
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("")
async def upload_file(
    file: Annotated[UploadFile, File()],
    db: Session = Depends(get_db)
):
    """
    Upload a CSV file for processing.
    
    Validates that the uploaded file is a CSV file, saves it, creates an UploadJob,
    and triggers the Celery task to process it.
    """
    # Validate file type
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    # Check content type
    if file.content_type and 'csv' not in file.content_type.lower():
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    try:
        # Generate unique filename
        file_id = str(uuid.uuid4())
        file_extension = os.path.splitext(file.filename)[1]
        saved_filename = f"{file_id}{file_extension}"
        file_path = os.path.join(UPLOAD_DIR, saved_filename)
        
        # Save file
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Create UploadJob record
        upload_job = UploadJob(
            task_id="",  # Will be updated after task is created
            status=UploadStatus.PENDING
        )
        db.add(upload_job)
        db.commit()
        db.refresh(upload_job)
        
        # Trigger Celery task
        task = process_csv_upload.delay(file_path, upload_job.id)
        
        # Update UploadJob with task_id
        upload_job.task_id = task.id
        db.commit()
        
        return {
            "message": "File uploaded successfully",
            "filename": file.filename,
            "upload_job_id": upload_job.id,
            "task_id": task.id,
            "status": "pending"
        }
        
    except Exception as e:
        # Clean up file if it was created
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/{task_id}/progress")
async def get_upload_progress(
    task_id: str,
    db: Session = Depends(get_db)
):
    """
    Get the progress of a CSV upload task.
    
    Returns progress information from both the Celery task and the UploadJob record.
    """
    # Get task result
    task_result = AsyncResult(task_id, app=celery_app)
    
    # Get UploadJob record
    upload_job = db.query(UploadJob).filter(UploadJob.task_id == task_id).first()
    
    if not upload_job:
        raise HTTPException(status_code=404, detail="Upload job not found")
    
    # Get task state and metadata
    task_state = task_result.state
    task_info = task_result.info if task_result.info else {}
    
    # Combine information from both sources
    response = {
        "task_id": task_id,
        "upload_job_id": upload_job.id,
        "status": upload_job.status.value,
        "progress": upload_job.progress,
        "total_rows": upload_job.total_rows,
        "processed_rows": upload_job.processed_rows,
        "task_state": task_state,
        "message": task_info.get("message", ""),
    }
    
    # Add error if failed
    if upload_job.status == UploadStatus.FAILED:
        response["error"] = upload_job.error_message
    elif task_state == "FAILURE":
        response["error"] = str(task_info.get("error", "Unknown error"))
    
    return response

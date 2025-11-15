import os
import uuid
import json
import asyncio
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
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
    Get the progress of a CSV upload task (one-time check).
    
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


@router.get("/{task_id}/stream")
async def stream_upload_progress(task_id: str):
    """
    Stream progress updates using Server-Sent Events (SSE).
    
    Provides real-time progress updates as the CSV is being processed.
    """
    from app.database import SessionLocal
    
    async def event_generator():
        last_progress = -1
        first_update = True
        
        while True:
            # Create a new database session for each check to ensure fresh data
            db = SessionLocal()
            try:
                # Get task result
                task_result = AsyncResult(task_id, app=celery_app)
                
                # Get UploadJob record (query fresh from database)
                # Use merge to ensure we get the latest data from the database
                upload_job = db.query(UploadJob).filter(UploadJob.task_id == task_id).first()
                
                if not upload_job:
                    yield f"data: {json.dumps({'error': 'Upload job not found'})}\n\n"
                    break
                
                # Expire the object to force a fresh read from database
                db.expire(upload_job)
                db.refresh(upload_job)
                
                # Get task state and metadata
                task_state = task_result.state
                task_info = task_result.info if task_result.info else {}
                
                # Prepare progress data
                # Use task_info as fallback if database values are not yet set
                current_progress = upload_job.progress if upload_job.progress is not None else (task_info.get("progress", 0) if task_info else 0)
                total_rows = upload_job.total_rows if upload_job.total_rows is not None else task_info.get("total_rows")
                processed_rows = upload_job.processed_rows if upload_job.processed_rows is not None else task_info.get("processed_rows", 0)
                
                # Send update if progress has changed OR if this is the first update
                if current_progress != last_progress or first_update:
                    first_update = False
                    progress_data = {
                        "task_id": task_id,
                        "upload_job_id": upload_job.id,
                        "status": upload_job.status.value,
                        "progress": current_progress,
                        "total_rows": total_rows,
                        "processed_rows": processed_rows,
                        "task_state": task_state,
                        "message": task_info.get("message", ""),
                    }
                    
                    # Add error if failed
                    if upload_job.status == UploadStatus.FAILED:
                        progress_data["error"] = upload_job.error_message
                    elif task_state == "FAILURE":
                        progress_data["error"] = str(task_info.get("error", "Unknown error"))
                    
                    yield f"data: {json.dumps(progress_data)}\n\n"
                    last_progress = current_progress
                
                # Check if task is complete or failed
                # Check both database status and Celery task state
                # Also check if all rows are processed (processed_rows == total_rows)
                all_rows_processed = (
                    total_rows is not None and 
                    processed_rows is not None and 
                    total_rows > 0 and 
                    processed_rows >= total_rows
                )
                
                is_completed = (
                    upload_job.status == UploadStatus.COMPLETED or 
                    task_state == 'SUCCESS' or
                    all_rows_processed  # If all rows processed, consider it complete
                )
                is_failed = (
                    upload_job.status == UploadStatus.FAILED or 
                    task_state == 'FAILURE'
                )
                
                if is_completed or is_failed:
                    # Send final progress update before closing
                    if is_completed:
                        # Ensure we show 100% progress
                        final_progress = 100.0
                        final_processed = processed_rows if processed_rows else upload_job.processed_rows
                        final_total = total_rows if total_rows else upload_job.total_rows
                        
                        final_progress_data = {
                            "task_id": task_id,
                            "upload_job_id": upload_job.id,
                            "status": "completed",
                            "progress": final_progress,
                            "total_rows": final_total,
                            "processed_rows": final_processed,
                            "task_state": task_state,
                            "message": task_info.get("message", "Processing completed successfully"),
                        }
                        yield f"data: {json.dumps(final_progress_data)}\n\n"
                    # Send done status and close
                    yield f"data: {json.dumps({'status': 'done'})}\n\n"
                    break
                
                # Wait before next check
                # Ideal SSE interval: 200-500ms
                # - Too frequent (<100ms): Unnecessary DB queries, high server load
                # - Too infrequent (>1000ms): Perceived lag, poor UX
                # 250ms is optimal: Smooth updates without excessive load
                await asyncio.sleep(0.25)  # 250ms - optimal balance
            finally:
                db.close()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable buffering for nginx
        }
    )

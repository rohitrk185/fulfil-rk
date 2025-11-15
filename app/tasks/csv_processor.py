import csv
import random
import io
import base64
import logging
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from celery.exceptions import SoftTimeLimitExceeded
from app.config import settings
from app.models.product import Product
from app.models.upload_job import UploadJob, UploadStatus
from celery_app import celery_app

# Set up logging
logger = logging.getLogger(__name__)

# Create database engine for Celery tasks
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dynamic chunk size based on file size
# - Small files (< 10k rows): 1,000 rows per chunk (more frequent updates)
# - Medium files (10k-100k rows): 10,000 rows per chunk (balanced)
# - Large files (100k+ rows): 10,000 rows per chunk (reduced from 20k for better reliability)
def get_chunk_size(total_rows: int) -> int:
    """
    Determine optimal chunk size based on total number of rows.
    
    Args:
        total_rows: Total number of rows in the CSV file
    
    Returns:
        Optimal chunk size for processing
    """
    if total_rows < 10000:
        return 1000  # Small files: more frequent updates
    elif total_rows < 100000:
        return 10000  # Medium files: balanced
    else:
        return 10000  # Large files (100k+): 10k chunks for better reliability


@celery_app.task(
    bind=True, 
    name="process_csv_upload",
    time_limit=120 * 60,  # 2 hours hard limit
    soft_time_limit=110 * 60  # 110 minutes soft limit
)
def process_csv_upload(self, file_content_base64: str, upload_job_id: int, filename: str = "upload.csv"):
    """
    Process CSV file and import products into database.
    
    Args:
        file_content_base64: Base64-encoded content of the uploaded CSV file (as string)
        upload_job_id: ID of the UploadJob record tracking this upload
        filename: Original filename (for logging purposes)
    
    Returns:
        dict: Result with status and message
    """
    db = SessionLocal()
    try:
        # Update task state
        self.update_state(state='PROCESSING', meta={'progress': 0, 'message': 'Starting CSV processing'})
        
        # Update UploadJob status
        upload_job = db.query(UploadJob).filter(UploadJob.id == upload_job_id).first()
        if not upload_job:
            raise ValueError(f"UploadJob with id {upload_job_id} not found")
        
        upload_job.status = UploadStatus.PROCESSING
        upload_job.progress = 0.0
        db.commit()
        db.refresh(upload_job)  # Refresh to ensure changes are visible
        
        # Decode base64-encoded file content back to bytes
        try:
            file_content = base64.b64decode(file_content_base64.encode('utf-8'))
        except Exception as e:
            raise ValueError(f"Failed to decode base64 file content: {str(e)}")
        
        # Convert file content (bytes) to text
        # Try UTF-8 first, fallback to latin-1 if needed
        try:
            file_text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning(f"UTF-8 decode failed for {filename}, trying latin-1")
            file_text = file_content.decode('latin-1')
        
        # Create StringIO object to read CSV from memory
        csv_file = io.StringIO(file_text)
        
        # Read and validate CSV
        self.update_state(state='PROCESSING', meta={'progress': 5, 'message': 'Reading CSV file'})
        
        # Count total rows first
        csv_file.seek(0)  # Reset to beginning
        reader = csv.DictReader(csv_file)
        total_rows = sum(1 for _ in reader)
        
        # Determine optimal chunk size based on file size
        chunk_size = get_chunk_size(total_rows)
        
        # Update total_rows immediately so SSE can see it
        upload_job.total_rows = total_rows
        upload_job.progress = 5.0  # Set initial progress
        db.commit()
        db.refresh(upload_job)  # Refresh to ensure changes are visible
        
        # Update Celery task state with total_rows info
        self.update_state(
            state='PROCESSING', 
            meta={
                'progress': 5, 
                'message': f'Found {total_rows} rows to process (chunk size: {chunk_size:,})', 
                'total_rows': total_rows,
                'processed_rows': 0
            }
        )
        
        # Validate required columns
        csv_file.seek(0)  # Reset to beginning
        reader = csv.DictReader(csv_file)
        headers = reader.fieldnames
        
        if not headers:
            raise ValueError("CSV file is empty or has no headers. Please ensure your CSV file has a header row with columns: name, sku, description")
        
        # Normalize headers (lowercase, strip whitespace)
        headers_lower = [h.lower().strip() for h in headers]
        required_columns = ['name', 'sku', 'description']
        
        missing_columns = []
        for col in required_columns:
            if col not in headers_lower:
                missing_columns.append(col)
        
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {', '.join(missing_columns)}. "
                f"Your CSV must have columns: name, sku, description. "
                f"Found columns: {', '.join(headers)}"
            )
        
        # Process CSV in chunks
        self.update_state(state='PROCESSING', meta={'progress': 10, 'message': 'Processing CSV data'})
        
        processed_rows = 0
        chunk = []
        rows_added_to_chunk = 0
        last_progress_update = 0
        # Progress update interval: Dynamic based on total_rows and chunk_size
        # Update more frequently for smaller files, less frequently for larger files
        if total_rows < 1000:
            PROGRESS_UPDATE_INTERVAL = 10
        elif total_rows < 10000:
            PROGRESS_UPDATE_INTERVAL = 50
        elif total_rows < 100000:
            PROGRESS_UPDATE_INTERVAL = 100
        else:
            # For large files (100k+), update every 1000 rows for better visibility
            PROGRESS_UPDATE_INTERVAL = 1000
        
        # Reset to beginning and process CSV
        csv_file.seek(0)
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            # Normalize column names (case-insensitive)
            normalized_row = {k.lower().strip(): v for k, v in row.items()}
            
            # Extract values
            sku = normalized_row.get('sku', '').strip()
            name = normalized_row.get('name', '').strip()
            description = normalized_row.get('description', '').strip()
            
            # Skip rows with missing required fields
            if not sku or not name:
                continue
            
            # Randomly assign active status
            active = random.choice([True, False])
            
            # Prepare product data
            product_data = {
                'sku': sku,
                'name': name,
                'description': description if description else None,
                'active': active
            }
            
            chunk.append(product_data)
            rows_added_to_chunk += 1
            
            # Update progress periodically (every PROGRESS_UPDATE_INTERVAL rows)
            # Do this BEFORE chunk processing to ensure progress is always visible
            if rows_added_to_chunk - last_progress_update >= PROGRESS_UPDATE_INTERVAL:
                progress = min(90, 10 + (rows_added_to_chunk / total_rows * 80))
                self.update_state(
                    state='PROCESSING',
                    meta={
                        'progress': progress,
                        'message': f'Processed {rows_added_to_chunk}/{total_rows} rows',
                        'processed_rows': rows_added_to_chunk,
                        'total_rows': total_rows
                    }
                )
                
                upload_job.progress = progress
                upload_job.processed_rows = rows_added_to_chunk
                db.commit()
                db.refresh(upload_job)  # Refresh to ensure changes are visible
                last_progress_update = rows_added_to_chunk
            
            # Process chunk when it reaches the determined chunk_size
            if len(chunk) >= chunk_size:
                try:
                    # Update progress BEFORE starting chunk processing
                    progress = min(90, 10 + (rows_added_to_chunk / total_rows * 80))
                    self.update_state(
                        state='PROCESSING',
                        meta={
                            'progress': progress,
                            'message': f'Processing chunk... ({rows_added_to_chunk}/{total_rows} rows read)',
                            'processed_rows': rows_added_to_chunk,
                            'total_rows': total_rows
                        }
                    )
                    upload_job.progress = progress
                    upload_job.processed_rows = rows_added_to_chunk
                    db.commit()
                    db.refresh(upload_job)
                    
                    logger.info(f"Processing chunk of {len(chunk)} products (total processed: {rows_added_to_chunk})")
                    _bulk_upsert_products(db, chunk, task_self=self, rows_processed=rows_added_to_chunk, total_rows=total_rows)
                    chunk = []  # Clear chunk after processing
                    logger.info(f"Chunk processed successfully. Continuing...")
                    
                    # Update progress after chunk processing
                    progress = min(90, 10 + (rows_added_to_chunk / total_rows * 80))
                    self.update_state(
                        state='PROCESSING',
                        meta={
                            'progress': progress,
                            'message': f'Processed {rows_added_to_chunk}/{total_rows} rows',
                            'processed_rows': rows_added_to_chunk,
                            'total_rows': total_rows
                        }
                    )
                    
                    upload_job.progress = progress
                    upload_job.processed_rows = rows_added_to_chunk
                    db.commit()
                    db.refresh(upload_job)  # Refresh to ensure changes are visible
                    last_progress_update = rows_added_to_chunk
                except Exception as chunk_error:
                    # Log the error but continue processing
                    logger.error(f"Error processing chunk at row {rows_added_to_chunk}: {str(chunk_error)}", exc_info=True)
                    # Rollback and try to continue with next chunk
                    db.rollback()
                    chunk = []  # Clear the problematic chunk
                    # Don't raise - continue processing remaining rows
                    # But update the error message
                    upload_job.error_message = f"Error at row {rows_added_to_chunk}: {str(chunk_error)[:200]}"
                    db.commit()
        
        # Process remaining chunk
        if chunk:
            _bulk_upsert_products(db, chunk, task_self=self, rows_processed=rows_added_to_chunk, total_rows=total_rows)
            # Update progress after processing final chunk
            # If all rows are processed, set progress to 95% (before final completion)
            if rows_added_to_chunk >= total_rows:
                progress = 95.0  # Almost done, finalizing
            else:
                progress = min(90, 10 + (rows_added_to_chunk / total_rows * 80))
            
            self.update_state(
                state='PROCESSING',
                meta={
                    'progress': progress,
                    'message': f'Processed {rows_added_to_chunk}/{total_rows} rows',
                    'processed_rows': rows_added_to_chunk,
                    'total_rows': total_rows
                }
            )
            
            upload_job.progress = progress
            upload_job.processed_rows = rows_added_to_chunk
            db.commit()
            db.refresh(upload_job)  # Refresh to ensure changes are visible
        
        # Use rows_added_to_chunk as the source of truth for processed_rows
        # This ensures we count exactly the number of rows we processed
        processed_rows = rows_added_to_chunk
        
        # Final update
        upload_job.status = UploadStatus.COMPLETED
        upload_job.progress = 100.0
        upload_job.processed_rows = processed_rows
        db.commit()
        db.refresh(upload_job)  # Refresh to ensure changes are visible
        
        self.update_state(
            state='SUCCESS',
            meta={
                'progress': 100,
                'message': f'Successfully processed {processed_rows} products',
                'processed_rows': processed_rows,
                'total_rows': total_rows
            }
        )
        
        return {
            'status': 'completed',
            'message': f'Successfully processed {processed_rows} products',
            'upload_job_id': upload_job_id,
            'processed_rows': processed_rows,
            'total_rows': total_rows
        }
        
    except SoftTimeLimitExceeded:
        # Handle soft time limit gracefully - mark as failed but don't crash
        try:
            upload_job = db.query(UploadJob).filter(UploadJob.id == upload_job_id).first()
            if upload_job:
                upload_job.status = UploadStatus.FAILED
                upload_job.error_message = "Processing exceeded time limit. The file may be too large. Please try splitting it into smaller files or contact support."
                db.commit()
        except Exception as db_error:
            logger.error(f"Error updating upload job status: {str(db_error)}", exc_info=True)
        
        # Update task state
        try:
            self.update_state(
                state='FAILURE', 
                meta={
                    'error': 'Processing exceeded time limit',
                    'exc_type': 'SoftTimeLimitExceeded',
                    'exc_message': 'The CSV file is too large and processing exceeded the time limit'
                }
            )
        except Exception:
            pass
        
        # Re-raise to mark task as failed
        raise
    except Exception as e:
        # Update UploadJob with error
        try:
            upload_job = db.query(UploadJob).filter(UploadJob.id == upload_job_id).first()
            if upload_job:
                upload_job.status = UploadStatus.FAILED
                upload_job.error_message = str(e)[:500]  # Limit error message length
                db.commit()
        except Exception as db_error:
            # Log but don't fail on database update error
            logger.error(f"Error updating upload job status: {str(db_error)}", exc_info=True)
        
        # Update task state to failed with proper error format
        try:
            error_message = str(e)[:500]  # Limit error message length
            self.update_state(
                state='FAILURE', 
                meta={
                    'error': error_message,
                    'exc_type': type(e).__name__,
                    'exc_message': error_message
                }
            )
        except Exception:
            # If update_state fails, just log it
            pass
        
        # Re-raise the exception
        raise
    finally:
        db.close()


def _bulk_upsert_products(db, products_data, task_self=None, rows_processed=0, total_rows=0):
    """
    Bulk upsert products using PostgreSQL ON CONFLICT.
    Handles case-insensitive SKU matching by normalizing SKU to lowercase.
    Also handles duplicates within the chunk (last occurrence wins).
    Uses PostgreSQL's native ON CONFLICT for efficient upserts without querying first.
    """
    if not products_data:
        return
    
    logger.debug(f"Starting bulk upsert for {len(products_data)} products...")
    
    # First, deduplicate within the chunk (keep last occurrence of each SKU)
    # This handles cases where the CSV has duplicate SKUs in the same chunk
    seen_skus = {}
    for product_data in products_data:
        sku_lower = product_data['sku'].lower()
        seen_skus[sku_lower] = product_data
    # Convert back to list (last occurrence of each SKU wins)
    deduplicated_data = list(seen_skus.values())
    logger.debug(f"After deduplication: {len(deduplicated_data)} products")
    
    # Normalize all SKUs to lowercase for case-insensitive matching
    for product_data in deduplicated_data:
        product_data['sku'] = product_data['sku'].lower()
    
    # Use PostgreSQL's ON CONFLICT for efficient bulk upsert
    # Process in batches to avoid memory and query size issues
    batch_size = 5000  # Process in batches of 5000
    num_batches = (len(deduplicated_data) + batch_size - 1) // batch_size
    
    for i in range(0, len(deduplicated_data), batch_size):
        batch = deduplicated_data[i:i + batch_size]
        batch_num = i // batch_size + 1
        logger.debug(f"Upserting batch {batch_num}/{num_batches} ({len(batch)} products)...")
        
        # Update progress during upsert if task_self is provided
        if task_self and total_rows > 0:
            progress = min(90, 10 + (rows_processed / total_rows * 80))
            task_self.update_state(
                state='PROCESSING',
                meta={
                    'progress': progress,
                    'message': f'Upserting products... ({rows_processed}/{total_rows} rows processed)',
                    'processed_rows': rows_processed,
                    'total_rows': total_rows
                }
            )
        
        try:
            # Use PostgreSQL's INSERT ... ON CONFLICT DO UPDATE
            # The unique constraint is on lower(sku) via the 'idx_sku_lower' index
            # For functional indexes, we need to specify the expression
            stmt = insert(Product).values(batch)
            
            # On conflict, update the existing row
            # Use the functional index expression: lower(sku)
            stmt = stmt.on_conflict_do_update(
                index_elements=[func.lower(Product.sku)],
                set_=dict(
                    name=stmt.excluded.name,
                    description=stmt.excluded.description,
                    active=stmt.excluded.active,
                    updated_at=func.now()
                )
            )
            
            db.execute(stmt)
            db.commit()
            logger.debug(f"Successfully upserted batch {batch_num}")
            
        except Exception as e:
            db.rollback()
            # If bulk upsert fails, fall back to individual upserts for this batch
            logger.warning(f"Bulk upsert failed for batch {batch_num}, falling back to individual upserts: {str(e)[:200]}", exc_info=True)
            
            for product_data in batch:
                try:
                    # Use ON CONFLICT for individual upserts
                    stmt = insert(Product).values(product_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[func.lower(Product.sku)],
                        set_=dict(
                            name=stmt.excluded.name,
                            description=stmt.excluded.description,
                            active=stmt.excluded.active,
                            updated_at=func.now()
                        )
                    )
                    db.execute(stmt)
                    db.commit()
                except Exception as individual_error:
                    # Skip duplicates that still occur
                    db.rollback()
                    logger.warning(f"Skipping product with SKU: {product_data.get('sku', 'unknown')} - {str(individual_error)[:100]}")
                    continue
    
    logger.debug("Bulk upsert completed successfully")

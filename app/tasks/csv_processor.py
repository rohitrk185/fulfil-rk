import csv
import random
import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from app.config import settings
from app.models.product import Product
from app.models.upload_job import UploadJob, UploadStatus
from celery_app import celery_app

# Create database engine for Celery tasks
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Chunk size for processing
# Ideal: 5,000-10,000 rows per chunk
# - Smaller chunks: More frequent progress updates, lower memory usage, but more DB transactions
# - Larger chunks: Fewer DB transactions, faster bulk operations, but less frequent updates
# For 500k rows: 10,000 is optimal (50 chunks total, good balance)
CHUNK_SIZE = 10000


@celery_app.task(bind=True, name="process_csv_upload")
def process_csv_upload(self, file_path: str, upload_job_id: int):
    """
    Process CSV file and import products into database.
    
    Args:
        file_path: Path to the uploaded CSV file
        upload_job_id: ID of the UploadJob record tracking this upload
    
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
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Read and validate CSV
        self.update_state(state='PROCESSING', meta={'progress': 5, 'message': 'Reading CSV file'})
        
        # Count total rows first
        total_rows = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_rows = sum(1 for _ in reader)
        
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
                'message': f'Found {total_rows} rows to process', 
                'total_rows': total_rows,
                'processed_rows': 0
            }
        )
        
        # Validate required columns
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            if not headers:
                raise ValueError("CSV file is empty or has no headers")
            
            # Normalize headers (lowercase, strip whitespace)
            headers_lower = [h.lower().strip() for h in headers]
            required_columns = ['name', 'sku', 'description']
            
            missing_columns = []
            for col in required_columns:
                if col not in headers_lower:
                    missing_columns.append(col)
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Process CSV in chunks
        self.update_state(state='PROCESSING', meta={'progress': 10, 'message': 'Processing CSV data'})
        
        processed_rows = 0
        chunk = []
        rows_added_to_chunk = 0
        last_progress_update = 0
        # Progress update interval: Dynamic based on total_rows
        # For small files (<1000 rows): Update every 10 rows
        # For medium files (1k-10k rows): Update every 50 rows
        # For large files (>10k rows): Update every 100-200 rows
        # This ensures smooth progress without excessive DB writes
        if total_rows < 1000:
            PROGRESS_UPDATE_INTERVAL = 10
        elif total_rows < 10000:
            PROGRESS_UPDATE_INTERVAL = 50
        else:
            PROGRESS_UPDATE_INTERVAL = 100  # For 500k rows, update every 100 rows (5000 updates total)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
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
                
                # Process chunk when it reaches CHUNK_SIZE
                if len(chunk) >= CHUNK_SIZE:
                    _bulk_upsert_products(db, chunk)
                    chunk = []  # Clear chunk after processing
                    
                    # Update progress
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
                
                # Update progress periodically (every PROGRESS_UPDATE_INTERVAL rows)
                elif rows_added_to_chunk - last_progress_update >= PROGRESS_UPDATE_INTERVAL:
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
            
            # Process remaining chunk
            if chunk:
                _bulk_upsert_products(db, chunk)
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
        
    except Exception as e:
        # Update UploadJob with error
        try:
            upload_job = db.query(UploadJob).filter(UploadJob.id == upload_job_id).first()
            if upload_job:
                upload_job.status = UploadStatus.FAILED
                upload_job.error_message = str(e)
                db.commit()
        except:
            pass
        
        # Update task state to failed
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise
    finally:
        db.close()


def _bulk_upsert_products(db, products_data):
    """
    Bulk upsert products using PostgreSQL ON CONFLICT.
    Handles case-insensitive SKU matching by normalizing SKU to lowercase.
    """
    if not products_data:
        return
    
    # Normalize SKUs to lowercase for case-insensitive matching
    skus_lower = [p['sku'].lower() for p in products_data]
    
    # Query existing products by lowercase SKU in bulk
    existing_products = {
        p.sku.lower(): p 
        for p in db.query(Product).filter(func.lower(Product.sku).in_(skus_lower)).all()
    }
    
    # Separate into updates and inserts
    to_update = []
    to_insert = []
    
    for product_data in products_data:
        sku_lower = product_data['sku'].lower()
        product_data['sku'] = sku_lower  # Normalize SKU to lowercase
        
        if sku_lower in existing_products:
            # Update existing
            existing = existing_products[sku_lower]
            existing.name = product_data['name']
            existing.description = product_data['description']
            existing.active = product_data['active']
            to_update.append(existing)
        else:
            # Insert new
            product = Product(**product_data)
            to_insert.append(product)
    
    # Bulk add new products
    if to_insert:
        db.bulk_save_objects(to_insert)
    
    # Updates are already tracked by SQLAlchemy session
    db.commit()

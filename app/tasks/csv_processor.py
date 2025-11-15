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
        db.commit()
        
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
        
        upload_job.total_rows = total_rows
        db.commit()
        
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
                processed_rows += 1
                
                # Process chunk when it reaches CHUNK_SIZE
                if len(chunk) >= CHUNK_SIZE:
                    _bulk_upsert_products(db, chunk)
                    chunk = []
                    
                    # Update progress
                    progress = min(90, 10 + (processed_rows / total_rows * 80))
                    self.update_state(
                        state='PROCESSING',
                        meta={
                            'progress': progress,
                            'message': f'Processed {processed_rows}/{total_rows} rows',
                            'processed_rows': processed_rows,
                            'total_rows': total_rows
                        }
                    )
                    
                    upload_job.progress = progress
                    upload_job.processed_rows = processed_rows
                    db.commit()
            
            # Process remaining chunk
            if chunk:
                _bulk_upsert_products(db, chunk)
                processed_rows += len(chunk)
        
        # Final update
        upload_job.status = UploadStatus.COMPLETED
        upload_job.progress = 100.0
        upload_job.processed_rows = processed_rows
        db.commit()
        
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

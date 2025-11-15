from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List
from app.database import get_db
from app.models.product import Product
from app.utils.webhook_trigger import trigger_webhooks
from pydantic import BaseModel, Field
from datetime import datetime

router = APIRouter(prefix="/api/products", tags=["products"])


# Pydantic models for request/response
class ProductResponse(BaseModel):
    id: int
    sku: str
    name: str
    description: Optional[str]
    active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    products: List[ProductResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ProductCreate(BaseModel):
    sku: str = Field(..., min_length=1, description="Product SKU (must be unique)")
    name: str = Field(..., min_length=1, description="Product name")
    description: Optional[str] = Field(None, description="Product description")
    active: bool = Field(True, description="Product active status")


class ProductUpdate(BaseModel):
    sku: Optional[str] = Field(None, min_length=1, description="Product SKU (must be unique)")
    name: Optional[str] = Field(None, min_length=1, description="Product name")
    description: Optional[str] = Field(None, description="Product description")
    active: Optional[bool] = Field(None, description="Product active status")


@router.get("", response_model=ProductListResponse)
async def get_products(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sku: Optional[str] = Query(None, description="Filter by SKU (partial match)"),
    name: Optional[str] = Query(None, description="Filter by name (partial match)"),
    description: Optional[str] = Query(None, description="Filter by description (partial match)"),
    active: Optional[bool] = Query(None, description="Filter by active status"),
    db: Session = Depends(get_db)
):
    """
    Get a paginated list of products with optional filtering.
    
    Supports filtering by:
    - sku: Partial match (case-insensitive)
    - name: Partial match (case-insensitive)
    - description: Partial match (case-insensitive)
    - active: Exact match (true/false)
    
    Returns paginated results with metadata.
    """
    # Build query with filters
    query = db.query(Product)
    
    # Apply filters
    if sku:
        query = query.filter(func.lower(Product.sku).contains(func.lower(sku)))
    
    if name:
        query = query.filter(func.lower(Product.name).contains(func.lower(name)))
    
    if description:
        query = query.filter(func.lower(Product.description).contains(func.lower(description)))
    
    if active is not None:
        query = query.filter(Product.active == active)
    
    # Get total count
    total = query.count()
    
    # Calculate pagination
    total_pages = (total + page_size - 1) // page_size  # Ceiling division
    
    # Apply pagination
    offset = (page - 1) * page_size
    products = query.order_by(Product.created_at.desc()).offset(offset).limit(page_size).all()
    
    return ProductListResponse(
        products=[ProductResponse.model_validate(p) for p in products],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@router.post("", response_model=ProductResponse, status_code=201)
async def create_product(
    product_data: ProductCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new product.
    
    Validates that the SKU is unique (case-insensitive).
    Returns the created product with 201 status.
    """
    # Check if SKU already exists (case-insensitive)
    existing_product = db.query(Product).filter(
        func.lower(Product.sku) == func.lower(product_data.sku)
    ).first()
    
    if existing_product:
        raise HTTPException(
            status_code=400,
            detail=f"Product with SKU '{product_data.sku}' already exists. SKUs must be unique (case-insensitive)."
        )
    
    # Create new product
    product = Product(
        sku=product_data.sku.lower(),  # Store SKU in lowercase for consistency
        name=product_data.name,
        description=product_data.description,
        active=product_data.active
    )
    
    db.add(product)
    db.commit()
    db.refresh(product)
    
    # Trigger webhooks for product.created event
    product_payload = {
        'id': product.id,
        'sku': product.sku,
        'name': product.name,
        'description': product.description,
        'active': product.active,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    }
    trigger_webhooks(db, 'product.created', product_payload)
    
    return ProductResponse.model_validate(product)


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a single product by ID.
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return ProductResponse.model_validate(product)


@router.put("/{product_id}", response_model=ProductResponse)
async def update_product(
    product_id: int,
    product_data: ProductUpdate,
    db: Session = Depends(get_db)
):
    """
    Update an existing product.
    
    Only provided fields will be updated.
    Validates that SKU is unique if being updated (case-insensitive).
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Check SKU uniqueness if SKU is being updated
    if product_data.sku is not None:
        existing_product = db.query(Product).filter(
            func.lower(Product.sku) == func.lower(product_data.sku),
            Product.id != product_id
        ).first()
        
        if existing_product:
            raise HTTPException(
                status_code=400,
                detail=f"Product with SKU '{product_data.sku}' already exists. SKUs must be unique (case-insensitive)."
            )
        product.sku = product_data.sku.lower()  # Store in lowercase
    
    # Update other fields if provided
    if product_data.name is not None:
        product.name = product_data.name
    
    if product_data.description is not None:
        product.description = product_data.description
    
    if product_data.active is not None:
        product.active = product_data.active
    
    db.commit()
    db.refresh(product)
    
    # Trigger webhooks for product.updated event
    product_payload = {
        'id': product.id,
        'sku': product.sku,
        'name': product.name,
        'description': product.description,
        'active': product.active,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    }
    trigger_webhooks(db, 'product.updated', product_payload)
    
    return ProductResponse.model_validate(product)


@router.delete("/{product_id}", status_code=204)
async def delete_product(
    product_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a product by ID.
    
    Returns 204 No Content on success.
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Store product data before deletion for webhook
    product_payload = {
        'id': product.id,
        'sku': product.sku,
        'name': product.name,
        'description': product.description,
        'active': product.active,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    }
    
    db.delete(product)
    db.commit()
    
    # Trigger webhooks for product.deleted event
    trigger_webhooks(db, 'product.deleted', product_payload)
    
    return None


@router.delete("/bulk", status_code=200)
async def bulk_delete_products(
    confirm: bool = Query(..., description="Confirmation flag (must be true)"),
    filter_active: Optional[bool] = Query(None, description="Optional: Delete only active/inactive products"),
    db: Session = Depends(get_db)
):
    """
    Delete multiple products.
    
    Requires confirmation query parameter set to true.
    Optionally filter by active status before deleting.
    
    Returns count of deleted products.
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set 'confirm=true' query parameter."
        )
    
    # Build query
    query = db.query(Product)
    
    # Apply filter if provided
    if filter_active is not None:
        query = query.filter(Product.active == filter_active)
    
    # Get count before deletion
    count = query.count()
    
    # Delete all matching products
    query.delete(synchronize_session=False)
    db.commit()
    
    return {
        "message": f"Successfully deleted {count} product(s)",
        "deleted_count": count
    }


from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from app.database import get_db
from app.models.webhook import Webhook
from pydantic import BaseModel, Field, HttpUrl, validator
from datetime import datetime

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


# Pydantic models for request/response
class WebhookResponse(BaseModel):
    id: int
    url: str
    event_types: List[str]
    enabled: bool
    secret: Optional[str] = None
    headers: Optional[dict] = None
    timeout: int
    retry_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WebhookCreate(BaseModel):
    url: str = Field(..., description="Webhook URL")
    event_types: List[str] = Field(..., description="List of event types to subscribe to")
    enabled: bool = Field(True, description="Whether the webhook is enabled")
    secret: Optional[str] = Field(None, description="Optional HMAC secret for webhook signature")
    headers: Optional[dict] = Field(None, description="Custom headers as key-value pairs")
    timeout: int = Field(30, ge=1, le=300, description="Request timeout in seconds")
    retry_count: int = Field(3, ge=0, le=10, description="Maximum retry attempts")

    @validator('url')
    def validate_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v

    @validator('event_types')
    def validate_event_types(cls, v):
        valid_events = ['product.created', 'product.updated', 'product.deleted']
        for event in v:
            if event not in valid_events:
                raise ValueError(f'Invalid event type: {event}. Must be one of {valid_events}')
        if not v:
            raise ValueError('At least one event type must be specified')
        return v


class WebhookUpdate(BaseModel):
    url: Optional[str] = Field(None, description="Webhook URL")
    event_types: Optional[List[str]] = Field(None, description="List of event types to subscribe to")
    enabled: Optional[bool] = Field(None, description="Whether the webhook is enabled")
    secret: Optional[str] = Field(None, description="Optional HMAC secret for webhook signature")
    headers: Optional[dict] = Field(None, description="Custom headers as key-value pairs")
    timeout: Optional[int] = Field(None, ge=1, le=300, description="Request timeout in seconds")
    retry_count: Optional[int] = Field(None, ge=0, le=10, description="Maximum retry attempts")

    @validator('url')
    def validate_url(cls, v):
        if v is not None and not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v

    @validator('event_types')
    def validate_event_types(cls, v):
        if v is not None:
            valid_events = ['product.created', 'product.updated', 'product.deleted']
            for event in v:
                if event not in valid_events:
                    raise ValueError(f'Invalid event type: {event}. Must be one of {valid_events}')
            if not v:
                raise ValueError('At least one event type must be specified')
        return v


@router.get("", response_model=List[WebhookResponse])
async def get_webhooks(
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    db: Session = Depends(get_db)
):
    """
    Get a list of all webhooks.
    
    Optionally filter by enabled status.
    """
    query = db.query(Webhook)
    
    if enabled is not None:
        query = query.filter(Webhook.enabled == enabled)
    
    webhooks = query.order_by(Webhook.created_at.desc()).all()
    return [WebhookResponse.model_validate(w) for w in webhooks]


@router.get("/{webhook_id}", response_model=WebhookResponse)
async def get_webhook(
    webhook_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a single webhook by ID.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    return WebhookResponse.model_validate(webhook)


@router.post("", response_model=WebhookResponse, status_code=201)
async def create_webhook(
    webhook_data: WebhookCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new webhook.
    
    Validates URL format and event types.
    """
    webhook = Webhook(
        url=webhook_data.url,
        event_types=webhook_data.event_types,
        enabled=webhook_data.enabled,
        secret=webhook_data.secret,
        headers=webhook_data.headers,
        timeout=webhook_data.timeout,
        retry_count=webhook_data.retry_count
    )
    
    db.add(webhook)
    db.commit()
    db.refresh(webhook)
    
    return WebhookResponse.model_validate(webhook)


@router.put("/{webhook_id}", response_model=WebhookResponse)
async def update_webhook(
    webhook_id: int,
    webhook_data: WebhookUpdate,
    db: Session = Depends(get_db)
):
    """
    Update an existing webhook.
    
    Only provided fields will be updated.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    # Update fields if provided
    if webhook_data.url is not None:
        webhook.url = webhook_data.url
    
    if webhook_data.event_types is not None:
        webhook.event_types = webhook_data.event_types
    
    if webhook_data.enabled is not None:
        webhook.enabled = webhook_data.enabled
    
    if webhook_data.secret is not None:
        webhook.secret = webhook_data.secret
    
    if webhook_data.headers is not None:
        webhook.headers = webhook_data.headers
    
    if webhook_data.timeout is not None:
        webhook.timeout = webhook_data.timeout
    
    if webhook_data.retry_count is not None:
        webhook.retry_count = webhook_data.retry_count
    
    db.commit()
    db.refresh(webhook)
    
    return WebhookResponse.model_validate(webhook)


@router.delete("/{webhook_id}", status_code=204)
async def delete_webhook(
    webhook_id: int,
    db: Session = Depends(get_db)
):
    """
    Delete a webhook by ID.
    
    Returns 204 No Content on success.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    db.delete(webhook)
    db.commit()
    
    return None


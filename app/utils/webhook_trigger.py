from sqlalchemy.orm import Session
from app.models.webhook import Webhook
from app.tasks.webhook_sender import send_webhook
from typing import Dict, Any


def trigger_webhooks(db: Session, event_type: str, payload: Dict[str, Any]):
    """
    Trigger webhooks for a given event type.
    
    This function queries all enabled webhooks that subscribe to the event type
    and queues Celery tasks to send the webhooks asynchronously.
    
    Args:
        db: Database session
        event_type: Type of event (e.g., 'product.created', 'product.updated', 'product.deleted')
        payload: Data to send in the webhook payload
    """
    # Query enabled webhooks that subscribe to this event type
    webhooks = db.query(Webhook).filter(
        Webhook.enabled == True
    ).all()
    
    # Filter webhooks that subscribe to this event type
    matching_webhooks = [
        w for w in webhooks
        if event_type in (w.event_types or [])
    ]
    
    # Queue Celery tasks for each matching webhook
    for webhook in matching_webhooks:
        send_webhook.delay(
            webhook_id=webhook.id,
            event_type=event_type,
            payload=payload
        )


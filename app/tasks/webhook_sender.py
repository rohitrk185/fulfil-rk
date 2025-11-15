import requests
import hmac
import hashlib
import json
import time
from typing import Dict, Any, Optional
from celery_app import celery_app
from app.config import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.webhook import Webhook

# Create database engine for Celery tasks
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@celery_app.task(bind=True, name="send_webhook", max_retries=3)
def send_webhook(
    self,
    webhook_id: int,
    event_type: str,
    payload: Dict[str, Any],
    attempt: int = 1
):
    """
    Send a webhook HTTP POST request.
    
    Args:
        webhook_id: ID of the webhook configuration
        event_type: Type of event (e.g., 'product.created')
        payload: Data to send in the webhook payload
        attempt: Current retry attempt number
    
    Returns:
        dict: Result with status, response_code, response_time, etc.
    """
    db = SessionLocal()
    try:
        # Get webhook configuration
        webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()
        
        if not webhook:
            return {
                'status': 'error',
                'error': f'Webhook {webhook_id} not found'
            }
        
        if not webhook.enabled:
            return {
                'status': 'skipped',
                'reason': 'Webhook is disabled'
            }
        
        # Prepare headers
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Fulfil-RK-Webhook/1.0',
            'X-Webhook-Event': event_type,
            'X-Webhook-ID': str(webhook_id)
        }
        
        # Add custom headers if configured
        if webhook.headers:
            headers.update(webhook.headers)
        
        # Prepare payload
        webhook_payload = {
            'event': event_type,
            'timestamp': time.time(),
            'data': payload
        }
        
        # Add HMAC signature if secret is configured
        if webhook.secret:
            payload_str = json.dumps(webhook_payload, sort_keys=True)
            signature = hmac.new(
                webhook.secret.encode('utf-8'),
                payload_str.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers['X-Webhook-Signature'] = f'sha256={signature}'
        
        # Send webhook
        start_time = time.time()
        try:
            response = requests.post(
                webhook.url,
                json=webhook_payload,
                headers=headers,
                timeout=webhook.timeout
            )
            response_time = time.time() - start_time
            
            # Check if request was successful
            if response.status_code >= 200 and response.status_code < 300:
                return {
                    'status': 'success',
                    'webhook_id': webhook_id,
                    'event_type': event_type,
                    'response_code': response.status_code,
                    'response_time': round(response_time, 3),
                    'attempt': attempt
                }
            else:
                # Retry on client/server errors
                if attempt < webhook.retry_count:
                    raise requests.exceptions.HTTPError(
                        f'HTTP {response.status_code}: {response.text[:200]}'
                    )
                else:
                    return {
                        'status': 'failed',
                        'webhook_id': webhook_id,
                        'event_type': event_type,
                        'response_code': response.status_code,
                        'response_time': round(response_time, 3),
                        'error': response.text[:500],
                        'attempt': attempt
                    }
        
        except requests.exceptions.Timeout:
            if attempt < webhook.retry_count:
                raise
            return {
                'status': 'failed',
                'webhook_id': webhook_id,
                'event_type': event_type,
                'error': 'Request timeout',
                'attempt': attempt
            }
        
        except requests.exceptions.RequestException as e:
            if attempt < webhook.retry_count:
                raise
            return {
                'status': 'failed',
                'webhook_id': webhook_id,
                'event_type': event_type,
                'error': str(e)[:500],
                'attempt': attempt
            }
    
    except Exception as e:
        # Retry with exponential backoff
        max_retries = webhook.retry_count if webhook else 3
        if attempt < max_retries:
            # Exponential backoff: 1s, 2s, 4s
            retry_delay = 2 ** (attempt - 1)
            raise self.retry(exc=e, countdown=retry_delay)
        
        return {
            'status': 'error',
            'webhook_id': webhook_id,
            'event_type': event_type,
            'error': str(e)[:500],
            'attempt': attempt
        }
    
    finally:
        db.close()


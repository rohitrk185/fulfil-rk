from celery import Celery
from app.config import settings
import ssl
from urllib.parse import urlparse, parse_qs, urlencode

# Parse Redis URL and handle SSL for Upstash
broker_url = settings.CELERY_BROKER_URL
backend_url = settings.CELERY_RESULT_BACKEND

# Normalize Redis URLs - remove database number if present (Upstash doesn't use it)
def normalize_redis_url(url):
    """Normalize Redis URL for Upstash compatibility - removes any path component and adds SSL params"""
    if not url:
        return url
    
    if url.startswith(("redis://", "rediss://")):
        try:
            # Parse the URL properly
            parsed = urlparse(url)
            # Reconstruct URL without path (Upstash doesn't use database numbers in path)
            # Format: scheme://netloc?query#fragment (no path)
            normalized = f"{parsed.scheme}://{parsed.netloc}"
            
            # For rediss:// URLs, add ssl_cert_reqs parameter if not present
            query_params = {}
            if parsed.query:
                # Parse existing query parameters
                query_params = {k: v[0] if v else '' for k, v in parse_qs(parsed.query).items()}
            
            # Add ssl_cert_reqs for rediss:// URLs
            if parsed.scheme == "rediss" and "ssl_cert_reqs" not in query_params:
                query_params["ssl_cert_reqs"] = "none"
            
            # Reconstruct query string
            if query_params:
                normalized += f"?{urlencode(query_params)}"
            
            if parsed.fragment:
                normalized += f"#{parsed.fragment}"
            return normalized
        except Exception:
            # Fallback to simple string manipulation if urlparse fails
            # Remove everything after the first / after host:port
            scheme_end = url.find('://') + 3
            if scheme_end > 2:
                path_start = url.find('/', scheme_end)
                base_url = url[:path_start] if path_start != -1 else url
                # Add ssl_cert_reqs for rediss:// if not present
                if url.startswith("rediss://") and "ssl_cert_reqs" not in base_url:
                    separator = "?" if "?" not in base_url else "&"
                    base_url += f"{separator}ssl_cert_reqs=none"
                return base_url
    return url

# Normalize URLs
broker_url = normalize_redis_url(broker_url)
backend_url = normalize_redis_url(backend_url)

# Handle rediss:// (SSL) URLs for Upstash
broker_transport_options = {}
result_backend_transport_options = {}

if broker_url.startswith("rediss://"):
    broker_transport_options = {
        "ssl_cert_reqs": ssl.CERT_NONE,
        "ssl_ca_certs": None,
    }

if backend_url.startswith("rediss://"):
    result_backend_transport_options = {
        "ssl_cert_reqs": ssl.CERT_NONE,
        "ssl_ca_certs": None,
    }

# Create Celery instance
celery_app = Celery(
    "fulfil_rk",
    broker=broker_url,
    backend=backend_url,
    include=["app.tasks.csv_processor", "app.tasks.webhook_sender"]
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    broker_transport_options=broker_transport_options,
    result_backend_transport_options=result_backend_transport_options,
)


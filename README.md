# Fulfil-RK - Product Management System

A web application for managing products with CSV import capabilities, real-time progress tracking, and webhook support. Built with FastAPI, PostgreSQL, Celery, and Redis.

## Features

- CSV Upload: Import up to 500,000 product records from CSV files
- Real-time Progress: Server-Sent Events (SSE) for live upload progress tracking
- Product Management: Full CRUD operations with filtering and pagination
- Webhook System: Automatic webhook delivery on product events (create, update, delete)
- Modern UI: Clean, responsive interface
- Asynchronous Processing: Background task processing with Celery
- Retry Logic: Automatic retry with exponential backoff for webhook delivery

## Technology Stack

- Backend: FastAPI (Python)
- Database: PostgreSQL
- Task Queue: Celery with Redis
- ORM: SQLAlchemy
- Migrations: Alembic
- Frontend: HTML, CSS, JavaScript (vanilla)
- Real-time: Server-Sent Events (SSE)

## Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Redis 6 or higher
- pip (Python package manager)

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd fulfil-rk
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables

Create a `.env` file in the root directory:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/fulfil_db

# Redis
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Application
SECRET_KEY=your-secret-key-change-in-production
DEBUG=True
ENVIRONMENT=development
HOST=0.0.0.0
PORT=8000
```

### 5. Set Up Database

```bash
# Create database (if not exists)
createdb fulfil_db

# Run migrations
alembic upgrade head
```

### 6. Start Redis

```bash
# On macOS (with Homebrew)
brew services start redis

# On Linux
sudo systemctl start redis

# Or run directly
redis-server
```

## Running the Application

### 1. Start FastAPI Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The application will be available at `http://localhost:8000`

### 2. Start Celery Worker

In a separate terminal:

```bash
celery -A celery_app worker --loglevel=info
```

### 3. Access the Application

- Web UI: http://localhost:8000
- API Documentation: http://localhost:8000/docs
- Alternative API Docs: http://localhost:8000/redoc

## Usage

### CSV Upload

1. Navigate to the "Upload CSV" section
2. Click "Choose File" or drag and drop a CSV file
3. The CSV must have the following columns:
   - `name` (required)
   - `sku` (required, must be unique)
   - `description` (optional)
4. Click "Upload CSV"
5. Monitor progress in real-time
6. Products are automatically imported with random `active` status

CSV Format Example:
```csv
name,sku,description
Product A,SKU-001,This is product A
Product B,SKU-002,This is product B
```

### Product Management

- View Products: Browse all products with pagination
- Filter Products: Filter by SKU, name, description, or active status
- Create Product: Click "Create Product" button
- Edit Product: Click the edit icon on any product
- Delete Product: Click the delete icon on any product
- Bulk Delete: Click "Bulk Delete" to delete all products

### Webhook Configuration

1. Navigate to the "Webhooks" section
2. Click "Create Webhook"
3. Fill in the form:
   - URL: Your webhook endpoint URL
   - Event Types: Select which events to subscribe to
     - `product.created` - Triggered when a product is created
     - `product.updated` - Triggered when a product is updated
     - `product.deleted` - Triggered when a product is deleted
   - Enabled: Toggle to enable/disable the webhook
   - Secret (optional): HMAC secret for webhook signature
   - Timeout: Request timeout in seconds (default: 30)
   - Retry Count: Maximum retry attempts (default: 3)
4. Click "Save"

Webhook Payload Format:
```json
{
  "event": "product.created",
  "timestamp": 1234567890.123,
  "data": {
    "id": 1,
    "sku": "SKU-001",
    "name": "Product A",
    "description": "Description",
    "active": true,
    "created_at": "2024-01-01T00:00:00",
    "updated_at": "2024-01-01T00:00:00"
  }
}
```

Webhook Headers:
- `Content-Type: application/json`
- `X-Webhook-Event: product.created` (or updated/deleted)
- `X-Webhook-ID: <webhook_id>`
- `X-Webhook-Signature: sha256=<signature>` (if secret is configured)

## API Endpoints

### Products

- `GET /api/products` - List products (with pagination and filtering)
- `GET /api/products/{id}` - Get single product
- `POST /api/products` - Create product
- `PUT /api/products/{id}` - Update product
- `DELETE /api/products/{id}` - Delete product
- `DELETE /api/products/bulk?confirm=true` - Bulk delete products

### Upload

- `POST /api/upload` - Upload CSV file
- `GET /api/upload/{task_id}/progress` - Get upload progress
- `GET /api/upload/{task_id}/stream` - Stream upload progress (SSE)

### Webhooks

- `GET /api/webhooks` - List webhooks
- `GET /api/webhooks/{id}` - Get single webhook
- `POST /api/webhooks` - Create webhook
- `PUT /api/webhooks/{id}` - Update webhook
- `DELETE /api/webhooks/{id}` - Delete webhook

## Project Structure

```
fulfil-rk/
├── app/
│   ├── api/
│   │   ├── products.py      # Product API endpoints
│   │   ├── upload.py        # Upload API endpoints
│   │   └── webhooks.py      # Webhook API endpoints
│   ├── models/
│   │   ├── product.py       # Product model
│   │   ├── upload_job.py    # Upload job model
│   │   └── webhook.py       # Webhook model
│   ├── tasks/
│   │   ├── csv_processor.py # CSV processing task
│   │   └── webhook_sender.py # Webhook delivery task
│   ├── utils/
│   │   └── webhook_trigger.py # Webhook triggering utility
│   ├── config.py            # Configuration
│   └── database.py          # Database setup
├── alembic/                 # Database migrations
├── static/
│   ├── css/
│   │   └── style.css        # Styles
│   └── js/
│       └── app.js           # Frontend JavaScript
├── templates/
│   └── index.html           # Main HTML template
├── uploads/                 # Uploaded CSV files (created automatically)
├── celery_app.py            # Celery configuration
├── main.py                  # FastAPI application
├── requirements.txt         # Python dependencies
└── .env                     # Environment variables (create this)
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:password@localhost:5432/fulfil_db` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `CELERY_BROKER_URL` | Celery broker URL | `redis://localhost:6379/0` |
| `CELERY_RESULT_BACKEND` | Celery result backend URL | `redis://localhost:6379/0` |
| `SECRET_KEY` | Secret key for application | `your-secret-key-change-in-production` |
| `DEBUG` | Debug mode | `True` |
| `ENVIRONMENT` | Environment (development/production) | `development` |
| `HOST` | Server host | `0.0.0.0` |
| `PORT` | Server port | `8000` |

### Database Configuration

The application uses PostgreSQL with the following models:
- products: Product information
- webhooks: Webhook configurations
- upload_jobs: CSV upload job tracking

### Redis Configuration

Redis is used for:
- Celery message broker
- Celery result backend
- Task queue management

## Development

### Running in Development Mode

```bash
# Terminal 1: FastAPI server with auto-reload
uvicorn main:app --reload

# Terminal 2: Celery worker
celery -A celery_app worker --loglevel=info

# Terminal 3: Celery beat (if needed for scheduled tasks)
celery -A celery_app beat --loglevel=info
```

### Database Migrations

```bash
# Create a new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

### Testing Webhooks

You can test webhooks in two ways:

**Method 1: Using the Test Button (Recommended)**
1. Navigate to the "Webhooks" section
2. Click the test button (🧪) next to any enabled webhook
3. The system will send a test event and display the response code and time

**Method 2: Using webhook.site**
1. Visit https://webhook.site
2. Copy your unique webhook URL
3. Create a webhook in the application using that URL
4. Click the test button or perform product operations (create/update/delete)
5. Check webhook.site to see incoming requests

## Performance Considerations

- CSV Processing: Processes 10,000 rows per chunk (for files 100k+ rows)
- Progress Updates: Updates every 1,000 rows for large files (dynamic based on file size)
- SSE Updates: 250ms interval for real-time progress streaming
- Database: Uses connection pooling and bulk operations with PostgreSQL ON CONFLICT
- Webhooks: Asynchronous delivery, doesn't block product operations
- Upsert Strategy: Uses PostgreSQL ON CONFLICT for efficient bulk upserts without querying first

## Troubleshooting

### Celery Worker Not Processing Tasks

- Ensure Redis is running
- Check Celery worker logs for errors
- Verify `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` are correct

### Database Connection Errors

- Verify PostgreSQL is running
- Check `DATABASE_URL` in `.env` file
- Ensure database exists: `createdb fulfil_db`

### Webhooks Not Triggering

- Verify webhook is enabled
- Check event types match the operation
- Review Celery worker logs for delivery errors
- Ensure webhook URL is accessible

### CSV Upload Issues

- Verify CSV format (name, sku, description columns)
- Check file size (should handle up to 500k rows)
- Review Celery worker logs for processing errors

# LoadGuard Update API

API service for managing and monitoring LoadGuard data updates.

## Features

- REST API for controlling and monitoring data updates
- Automated dataset updates from multiple sources (Socrata, SMS, FTP)
- Webhook notifications for update events
- Scheduler management
- Mouse automation control

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/loadguardupdate-api.git
cd loadguardupdate-api
```

2. Create and activate virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Copy .env.example to .env and configure:

```bash
cp .env.example .env
```

5. Initialize the database:

```bash
alembic upgrade head
```

## Usage

Start the API server:

```bash
uvicorn api.main:app --reload
```

API documentation will be available at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Configuration

All configuration is done through environment variables. See `.env.example` for available options.

## API Endpoints

- `GET /api/status` - Get current system status
- `POST /api/control/start` - Start update service
- `POST /api/control/stop` - Stop update service
- `GET /api/settings` - Get current settings
- `POST /api/settings` - Update settings
- `GET /api/
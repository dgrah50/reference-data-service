# Reference Data Service

A service that aggregates and normalizes reference data from multiple cryptocurrency exchanges. The service provides both REST and WebSocket endpoints for accessing the data.

## Supported Exchanges

- Binance
- Bitget
- Bybit
- Coinbase
- Coinbase International
- OKX

## Features

- Fetches and normalizes reference data from multiple exchanges
- Provides a unified data model for trading pairs and exchange information
- REST API for retrieving full snapshots
- WebSocket API for real-time updates
- Circuit breaker pattern for fault tolerance
- Rate limiting for API calls
- Configurable exchange watchers
- Comprehensive logging

## Installation

Install dependencies:
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Usage

### Running the Service

Start the service with:
```bash
uv run python -m reference_data
```

The service will start on `http://localhost:8000`.

### API Endpoints

#### REST API

- `GET /snapshot`: Get the current snapshot of reference data from all exchanges
  - Returns a dictionary mapping exchange names to their current state
  - Returns 503 if no snapshot is available yet

#### WebSocket API

- `ws://localhost:8000/ws`: Subscribe to real-time updates
  - Sends full snapshots for initial state
  - Sends full state of all exchanges for each update

### Example Response

```json
{
  "Binance": {
    "exchange": "Binance",
    "timestamp": 1234567890,
    "trading_pairs": [
      {
        "symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "status": "TRADING",
        "tick_size": 0.1,
        "lot_size": 0.001
      }
    ]
  }
}
```

## Configuration

Exchange watchers can be configured via `config/watchers.yaml`.

## Development

### Project Structure

```
.
├── src/reference_data/
│   ├── adapters/          # Exchange-specific adapters
│   │   ├── base.py        # Base adapter class
│   │   ├── binance.py
│   │   ├── bitget.py
│   │   ├── bybit.py
│   │   ├── coinbase.py
│   │   ├── coinbase_intl.py
│   │   ├── okx.py
│   │   ├── config.py      # Adapter configuration
│   │   └── state.py       # State management
│   ├── api/               # REST and WebSocket API
│   │   ├── app.py
│   │   └── models.py
│   ├── core/              # Core functionality
│   │   ├── circuit_breaker.py
│   │   ├── rate_limiter.py
│   │   ├── service.py
│   │   └── watcher.py
│   └── utils/
│       └── logging.py
├── tests/                 # Unit tests
├── tests/integration/     # Integration tests
├── config/                # Configuration files
├── pyproject.toml
└── uv.lock
```

### Running Tests

Run unit tests:
```bash
pytest tests/ --ignore=tests/integration
```

Run integration tests:
```bash
pytest tests/integration/
```

Run all tests:
```bash
pytest
```

### Type Checking

```bash
mypy src
```

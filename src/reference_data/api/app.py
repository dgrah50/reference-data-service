"""
FastAPI application for serving reference data.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel


from ..api.models import ExchangeEnum, StandardExchangeInfo


class ExchangeHealth(BaseModel):
    """Health status for a single exchange."""
    status: str  # "healthy", "stale", "unavailable"
    last_update: Optional[int] = None  # Unix timestamp ms
    pair_count: int = 0
    circuit_breaker: Optional[str] = None  # "closed", "open", "half_open"


class HealthResponse(BaseModel):
    """Overall health response."""
    status: str  # "healthy", "degraded", "unhealthy"
    exchanges: Dict[str, ExchangeHealth]


from ..core.service import ReferenceDataService


# Service will be initialized in the lifespan context
service: ReferenceDataService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    global service
    service = ReferenceDataService()
    await service.start()
    yield
    # Shutdown
    if service is not None:
        await service.stop()


app = FastAPI(
    title="Reference Data Service",
    description="Service providing reference data from multiple crypto (Binance / OKX) exchanges",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get(
    "/snapshot",
    response_model=Dict[ExchangeEnum, StandardExchangeInfo],
    summary="Get current snapshot",
    description="Get the current snapshot of reference data from all exchanges.",
    responses={503: {"description": "No snapshot available yet", "model": dict}},
)
async def get_snapshot() -> Dict[ExchangeEnum, StandardExchangeInfo] | JSONResponse:
    """
    REST endpoint to get the current snapshot.
    Returns a 503 status if no snapshot is available yet.
    """
    if service is None:
        return JSONResponse(status_code=503, content={"error": "Service not initialized yet."})

    snapshot = service.get_snapshot()
    if not snapshot:
        return JSONResponse(status_code=503, content={"error": "No snapshot available yet."})
    return snapshot


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Get health status of the service and all exchanges.",
)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.
    Returns status of service and each exchange.

    Exchange status:
    - healthy: Updated within last 10 minutes
    - stale: Updated more than 10 minutes ago
    - unavailable: No data received yet
    """
    if service is None:
        return HealthResponse(status="unhealthy", exchanges={})

    now = int(time.time() * 1000)
    stale_threshold = 10 * 60 * 1000  # 10 minutes in ms

    exchanges: Dict[str, ExchangeHealth] = {}
    healthy_count = 0

    for exchange in ExchangeEnum:
        snapshot = service.latest_snapshot.get(exchange)
        adapter = service.adapters.get(exchange)

        # Get circuit breaker status if adapter exists
        cb_status = None
        if adapter and hasattr(adapter, 'circuit_breaker'):
            cb_status = adapter.circuit_breaker.state.value

        if snapshot is None:
            exchanges[exchange.value] = ExchangeHealth(
                status="unavailable",
                circuit_breaker=cb_status,
            )
        else:
            age = now - snapshot.timestamp
            is_healthy = age < stale_threshold

            # If circuit breaker is open, mark as degraded even if data is fresh
            status = "healthy" if is_healthy else "stale"
            if cb_status == "open":
                status = "degraded"

            exchanges[exchange.value] = ExchangeHealth(
                status=status,
                last_update=snapshot.timestamp,
                pair_count=len(snapshot.trading_pairs),
                circuit_breaker=cb_status,
            )

            if is_healthy and cb_status != "open":
                healthy_count += 1

    # Overall status based on exchange health
    total_enabled = len(service.adapters)
    if healthy_count == total_enabled:
        overall_status = "healthy"
    elif healthy_count > 0:
        overall_status = "degraded"
    else:
        overall_status = "unhealthy"

    return HealthResponse(status=overall_status, exchanges=exchanges)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint that streams exchange info updates.
    Sends the complete state of all exchanges in a single message for each update.
    """
    if service is None:
        await websocket.close(code=1013)
        return

    await websocket.accept()

    try:
        # Send initial snapshot
        current = service.get_snapshot()
        await websocket.send_json(
            {"exchanges": {exchange.value: state.model_dump() for exchange, state in current.items()}}
        )

        queue: asyncio.Queue[StandardExchangeInfo | None] = asyncio.Queue()

        def on_update(info: StandardExchangeInfo) -> None:
            queue.put_nowait(info)

        unsubscribe = service.subscribe(on_update)

        latest_state = current.copy()

        while True:
            update = await queue.get()
            if update is None:
                break
            latest_state[update.exchange] = update
            # Send complete state
            await websocket.send_json(
                {
                    "exchanges": {
                        exchange.value: state.model_dump() for exchange, state in latest_state.items()
                    }
                }
            )

    except WebSocketDisconnect:
        logging.info("WebSocket client disconnected")
    except Exception as e:
        logging.error("WebSocket error: %s", e)
    finally:
        if "unsubscribe" in locals():
            unsubscribe()
        if "queue" in locals():
            queue.put_nowait(None)
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        await websocket.close()

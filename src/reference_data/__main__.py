"""
Main entry point for running the reference data service.
"""

import uvicorn

from .api.app import app
from .utils.logging import configure_logging


def main() -> None:
    """Run the reference data service."""
    configure_logging()
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        lifespan="on",
    )


if __name__ == "__main__":
    main()

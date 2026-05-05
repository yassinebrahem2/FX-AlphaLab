"""API route handlers.

Expose submodules as package attributes so tests and imports can reference
`src.backend.routers.inference`, `src.backend.routers.ohlcv`, etc.
"""

from . import (
    inference,  # noqa: F401
    ohlcv,  # noqa: F401
    reports,  # noqa: F401
    signals,  # noqa: F401
    trades,  # noqa: F401
)

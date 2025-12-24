# services.py
# Compatibility shim to preserve the original "services" module namespace
# used by scanners.py. Re-exports from services_core and provides a few
# compatibility constants/templates scanners expects.

# Re-export everything from services_core for backward compatibility.
# Scanners imports names like: merge_into_cache, persist_raw_ws, log,
# TF_MAP, REVERSE_TF_MAP, ensure_cached_candles, detect_flip, add_signal, etc.
from services_core import *  # noqa: F401,F403

# Provide a sensible default root-signals log interval if not already present.
try:
    ROOT_SIGNALS_LOG_INTERVAL  # type: ignore
except NameError:
    ROOT_SIGNALS_LOG_INTERVAL = 60

# Candidate public websocket topic templates used by scanners.PublicWebsocketManager.connect_and_detect
# Templates must accept `.format(interval=..., symbol=...)`
CANDIDATE_PUBLIC_TEMPLATES = [
    "klineV2.{interval}.{symbol}",
    "kline.{interval}.{symbol}",
]

# Expose the MACD helpers and MTF functions from services_mtf if available.
# scanners may import detect_flip/flip_is_stable_enough from services (via the re-export above),
# but ensure they exist even if services_core wrappers are altered.
try:
    from services_mtf import detect_flip as _detect_flip  # type: ignore
    from services_mtf import flip_is_stable_enough as _flip_is_stable_enough  # type: ignore
    # Only set these names if they aren't present already from services_core import *
    globals().setdefault("detect_flip", _detect_flip)
    globals().setdefault("flip_is_stable_enough", _flip_is_stable_enough)
except Exception:
    # If services_mtf is not importable at shim import time, services_core provides wrappers that will import it later.
    pass

# Keep an explicit __all__ for clarity (optional)
__all__ = [
    # core names (a subset — scanners will import as needed via the top-level import)
    "merge_into_cache",
    "persist_raw_ws",
    "log",
    "TF_MAP",
    "REVERSE_TF_MAP",
    "ensure_cached_candles",
    "detect_flip",
    "flip_is_stable_enough",
    "add_signal",
    "remove_signal",
    "notify_alignment_if_ready",
    "get_tradable_usdt_symbols",
    "active_root_signals",
    "public_ws",
    "SCAN_INTERVAL_SECONDS",
    "DISCOVERY_CONCURRENCY",
    "ROOT_SIGNALS_LOG_INTERVAL",
    "CANDIDATE_PUBLIC_TEMPLATES",
    # re-export everything else as needed
]
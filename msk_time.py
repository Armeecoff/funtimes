from datetime import datetime, timezone, timedelta

MSK = timezone(timedelta(hours=3))

def now_msk() -> datetime:
    """Current Moscow time (UTC+3)."""
    return datetime.now(tz=MSK)

def strftime_msk(fmt: str, ts: int) -> str:
    """Format a Unix timestamp as Moscow time string."""
    return datetime.fromtimestamp(ts, tz=MSK).strftime(fmt)

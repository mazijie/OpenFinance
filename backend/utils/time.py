"""Datetime helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime, time


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize datetime to UTC."""

    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def day_start_utc(value: date) -> datetime:
    """Return UTC day start."""

    return datetime.combine(value, time.min, tzinfo=UTC)


def day_end_utc(value: date) -> datetime:
    """Return UTC day end."""

    return datetime.combine(value, time.max, tzinfo=UTC)


def parse_possible_datetime(value: str | None) -> datetime | None:
    """Parse common provider datetime formats into UTC."""

    if not value:
        return None

    candidate = value.strip()
    fmts = (
        "%Y%m%dT%H%M%SZ",
        "%Y%m%d%H%M%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    )
    for fmt in fmts:
        try:
            parsed = datetime.strptime(candidate, fmt)
            return ensure_utc(parsed)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ensure_utc(parsed)

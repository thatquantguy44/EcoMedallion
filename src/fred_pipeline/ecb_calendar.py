"""ECB/TARGET operating-day helpers shared by ingestion operations."""

from __future__ import annotations

from datetime import date, timedelta


def easter_sunday(year: int) -> date:
    """Return Gregorian Easter Sunday using the Meeus/Jones/Butcher method."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday_adjustment = (32 + 2 * e + 2 * i - h - k) % 7
    correction = (a + 11 * h + 22 * weekday_adjustment) // 451
    shifted = h + weekday_adjustment - 7 * correction + 114
    return date(year, shifted // 31, shifted % 31 + 1)


def is_target_operating_day(day: date) -> bool:
    """Whether TARGET is normally open on ``day``."""
    if day.weekday() >= 5:
        return False
    easter = easter_sunday(day.year)
    holidays = {
        date(day.year, 1, 1),
        easter - timedelta(days=2),
        easter + timedelta(days=1),
        date(day.year, 5, 1),
        date(day.year, 12, 25),
        date(day.year, 12, 26),
    }
    return day not in holidays

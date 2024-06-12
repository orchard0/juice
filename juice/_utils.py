from datetime import datetime, timedelta, timezone
from pytz import timezone as pytzTimezone


def parse_date(date=None, add=0):

    utc = pytzTimezone("UTC")

    if not date:
        result = utc.localize(datetime.now()).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=add)
    elif isinstance(date, str):
        result = utc.localize(datetime.strptime(date, "%Y-%m-%d")) + timedelta(days=add)

    elif isinstance(date, datetime):
        result = utc.localize(
            date.replace(hour=0, minute=0, second=0, microsecond=0)
        ) + timedelta(days=add)

    else:
        raise TypeError(
            "date must be either None, str in YYYY-MM-DD format or datetime."
        )

    return result


def format_date(date):
    if isinstance(date, float):
        return "Infinity"
    else:
        return date.strftime("%Y-%m-%d")

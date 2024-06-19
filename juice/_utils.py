from datetime import datetime, timedelta
from pytz import timezone


def _parse_date(date=None, add=0):

    utc = timezone("UTC")

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


def _format_date(date):
    london = timezone("Europe/London")
    if isinstance(date, float):
        return "Infinity"
    else:
        return date.astimezone(london).strftime("%Y-%m-%d")

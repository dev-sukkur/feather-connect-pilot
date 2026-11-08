"""Words for the pages, shared by the services and the templates."""

from datetime import timedelta


def span(duration: timedelta | int) -> str:
    """A length of time (or a number of seconds) in words: `15 min`."""
    seconds = int(duration.total_seconds() if isinstance(duration, timedelta) else duration)
    for size, unit in ((86400, "d"), (3600, "h"), (60, "min")):
        if seconds >= size and seconds % size == 0:
            return f"{seconds // size} {unit}"
    return f"{seconds} s" if seconds < 60 else f"{seconds // 60} min {seconds % 60} s"

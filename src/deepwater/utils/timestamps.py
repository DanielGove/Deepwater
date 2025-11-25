import numba as _nb
import datetime as _dt

# -----------------------------------------------------------------------------
# Microsecond timestamps: fast parse + formatting helpers.
# Coinbase WS emits 6 fractional digits; we store everything in microseconds.
# -----------------------------------------------------------------------------

@_nb.njit(cache=True, fastmath=True)
def _days_from_civil(y: int, m: int, d: int) -> int:
    y -= m <= 2
    era = (y if y >= 0 else 400) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + yoe // 400 + doy
    return era * 146097 + doe - 719468


@_nb.njit(cache=True, fastmath=True)
def parse_us_timestamp(ts) -> int:
    """
    Fast ASCII-only parser for timestamps like YYYY-MM-DDTHH:MM:SS.ffffffZ.
    Returns integer microseconds since epoch.
    """
    def d2(i): return (ts[i] - 48) * 10 + (ts[i + 1] - 48)
    def d4(i): return (ts[i] - 48) * 1000 + (ts[i + 1] - 48) * 100 + (ts[i + 2] - 48) * 10 + (ts[i + 3] - 48)

    year = d4(0); month = d2(5); day = d2(8)
    hour = d2(11); minute = d2(14); sec = d2(17)
    days = _days_from_civil(year, month, day)
    secs = days * 86400 + hour * 3600 + minute * 60 + sec

    # Fractional seconds up to 6 digits (microsecond resolution).
    end = len(ts)
    if end > 0 and ts[end - 1] == 90:  # 'Z'
        end -= 1
    frac = 0
    digits = 0
    i = 20
    while i < end and digits < 6:
        c = ts[i]
        if c < 48 or c > 57:
            break
        frac = frac * 10 + (c - 48)
        digits += 1
        i += 1
    while digits < 6:
        frac *= 10
        digits += 1

    return secs * 1_000_000 + frac


def us_to_iso(ts_us: int, *, with_z: bool = True, precision: int = 6) -> str:
    """
    Convert microseconds since epoch to an ISO-8601 UTC string.
    precision: number of fractional digits (0..6).
    """
    if precision < 0 or precision > 6:
        raise ValueError("precision must be between 0 and 6")
    seconds, micros = divmod(int(ts_us), 1_000_000)
    dt = _dt.datetime.utcfromtimestamp(seconds)
    if precision == 0:
        frac = ""
    else:
        frac = f".{int(micros):06d}"[: precision + 1]
    suffix = "Z" if with_z else ""
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + frac + suffix


# Backward-compatibility aliases (deprecated; now microseconds)
parse_ns_timestamp = parse_us_timestamp
ns_to_iso = us_to_iso

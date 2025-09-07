import numba as _nb

@_nb.njit(cache=True, fastmath=True)
def _days_from_civil(y: int, m: int, d:int):
    y -= m <=  2
    era = (y if y >= 0 else 400) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + yoe // 400 + doy
    return era * 146097 + doe - 719468

@_nb.njit(cache=True, fastmath=True)
def parse_ns_timestamp(ts):
    # digits to ints without slicing
    def d2(i): return (ts[i]-48)*10+(ts[i+1]-48)
    def d4(i): return (ts[i]-48)*1000+(ts[i+1]-48)*100+(ts[i+2]-48)*10+(ts[i+3]-48)
    year = d4(0); month = d2(5); day = d2(8)
    hour = d2(11); minute = d2(14); sec = d2(17)
    days = _days_from_civil(year, month, day)
    secs = days*86400+hour*3600+minute*60+sec
    nanos = 10000*d2(20)+100*d2(22)#+d2(24)
    return secs*1_000_000_000 + nanos
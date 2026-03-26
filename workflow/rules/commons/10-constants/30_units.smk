"""Module to define
enum types for common units.
These classes are used for
implementing, e.g., scaling
behavior for memory and runtime.

Note that Python keywords are forbidden:
- TimeUnit has no "min" for minute
- MemoryUnit has no "bytes" for byte(s)

"""

import enum


class TimeUnit(enum.Enum):
    HOUR = 1
    hour = 1
    hours = 1
    hrs = 1
    h = 1
    MINUTE = 2
    minute = 2
    minutes = 2
    m = 2
    SECOND = 3
    second = 3
    seconds = 3
    sec = 3
    s = 3


class MemoryUnit(enum.Enum):
    BYTE = 0
    byte = 0
    b = 0
    B = 0
    KiB = 1
    kib = 1
    kb = 1
    KB = 1
    k = 1
    K = 1
    kibibyte = 1
    MiB = 2
    mib = 2
    mb = 2
    MB = 2
    m = 2
    M = 2
    mebibyte = 2
    GiB = 3
    gib = 3
    gb = 3
    GB = 3
    g = 3
    G = 3
    gibibyte = 3
    TiB = 4
    tib = 4
    tb = 4
    TB = 4
    t = 4
    T = 4
    tebibyte = 4

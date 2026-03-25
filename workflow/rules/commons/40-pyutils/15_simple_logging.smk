"""This module implements simple
'conversion' type of functions to
perform simple yet common domain-agnostic
data (structure) conversions/transformations.
As a rule of thumb for developers,
functions in this module should require
only minimal input (parameters), provide
a well-defined output --- potentially as a
complex datatype, but simpler than the input ---
and should not call other custom functions in
their body.
"""

import sys


def logerr(msg):
    """
    Log a message to sys.stderr
    """
    level = "ERROR"
    if VERBOSE:
        level = "VERBOSE (err/dbg)"
    write_log_message(sys.stderr, level, msg)
    return


def log_err(msg):
    """
    Log a message to sys.stderr
    Calls 'logerr' internally.
    """
    _ = logerr(msg)
    return


def logout(msg):
    """
    Log a message to sys.stdout
    """
    write_log_message(sys.stdout, "INFO", msg)
    return


def log_out(msg):
    """
    Log a message to sys.stdout
    Calls 'logerr' internally.
    """
    _ = logout(msg)
    return


def write_log_message(stream, level, message):
    """
    Log a message with info LEVEL to STREAM
    (must support '.write' method).
    By default, MESSAGE is augmented w/ a timestamp.

    Level should be informative such as ERROR,
    WARNING, DEBUG and so on but is not sanity-checked.
    """
    # format: ISO 8601
    ts = get_timestamp()
    fmt_msg = f"{ts} - LOG {level}\n{message.strip()}\n"
    stream.write(fmt_msg)
    return

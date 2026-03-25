"""This module implements simple
'getter' type of functions to obtain
some small piece of information.
As a rule of thumb for developers,
functions in this module should require
only minimal input (parameters), provide
a well-defined (simple) output
--- preferably as a primitive datatype ---
and should not call other custom functions in
their body.
"""

import datetime
import getpass
import pathlib
import socket


def get_username():
    """
    return: login name of user as str
    """
    user = getpass.getuser()
    return user


def get_hostname():
    """
    return: host name as str
    """
    host = socket.gethostname()
    return host


def get_timestamp():
    """
    Get naive (not timezone-aware)
    timestamp representing 'now'.
    The formatting is following
    ISO 8601 w/o timezone offset, i.e.
    YYYY-MM-DDThh-mm-ss
    (24-hour format for time)

    return: 'now' timestamp w/o tz info as str
    """
    # format: ISO 8601
    ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return ts


def get_script(script_name, extension="py"):
    """Synonym introduced for naming coherence.
    The original 'find_script' should be removed
    in one of the future major updates.
    """
    predicate = lambda s: script_name == s.stem or script_name == s.name

    # DIR_SCRIPTS is set in common/constants
    all_scripts = DIR_SCRIPTS.glob(f"**/*.{extension.strip('.')}")
    retained_scripts = list(map(str, filter(predicate, all_scripts)))
    if len(retained_scripts) != 1:
        if len(retained_scripts) == 0:
            err_msg = (
                f"No scripts found or retained starting at '{DIR_SCRIPTS}' "
                f" and looking for '{script_name}' [+ .{extension}]"
            )
        else:
            ambig_scripts = "\n".join(retained_scripts)
            err_msg = f"Ambiguous script name '{script_name}':\n{ambig_scripts}\n"
        raise ValueError(err_msg)
    selected_script = retained_scripts[0]

    return selected_script


def find_script(script_name, extension="py"):
    """Original name of functionality; to be
    deprecated, kept for backwards compatibility.
    """
    warn_msg = (
        "commons::40-pyutils::00_simple_get.smk\n"
        "=== Deprecation warning ===\n"
        "The function 'find_script' is deprecated "
        "and will be removed in a future update.\n"
        "Call 'get_script(script_name, extension)' instead.\n"
        "===========================\n"
    )
    logerr(warn_msg)
    return get_script(script_name, extension)


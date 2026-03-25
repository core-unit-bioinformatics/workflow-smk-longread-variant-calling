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

def flatten_nested_paths(struct):
    """Given an arbitrarily nested structure
    of items assumed to be Paths, return
    a flattened version containing only
    pathlib.Path objects.

    dev info:
    This function is called by the accounting
    utility functions.

    return: list of pathlib.Path
    """

    flattened = set()
    for item in struct:
        if isinstance(item, str):
            flattened.add(pathlib.Path(item))
        elif isinstance(item, pathlib.Path):
            flattened.add(item)
        elif hasattr(item, "__iter__"):
            flattened = flattened.union(flatten_nested_paths(item))
        else:
            raise ValueError(f"Cannot handle item: {item}")
    return sorted(flattened)

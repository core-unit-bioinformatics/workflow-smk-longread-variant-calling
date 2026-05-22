"""This module is a temp fix;
the build_constraint function will
become part of the template commons
module tree
"""

def _build_constraint(values):
    escaped_values = sorted(map(re.escape, map(str, values)))
    constraint = "(" + "|".join(escaped_values) + ")"
    return constraint

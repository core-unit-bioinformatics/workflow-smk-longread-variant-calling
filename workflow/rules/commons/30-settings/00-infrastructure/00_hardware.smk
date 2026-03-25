"""Module containing global infrastructure
limits for CPU (cores) and memory that must
be respected by all jobs send to a (cluster)
scheduler. Compliance must be checked in the
calling scope, i.e., typically in the resources
section of a snakemake rule.
"""

# CPU limits
CPU_LOW = config.get(
    OPTIONS.cpu_low.name, OPTIONS.cpu_low.default
)
assert isinstance(CPU_LOW, int)

CPU_MEDIUM = config.get(
    OPTIONS.cpu_medium.name, OPTIONS.cpu_medium.default
)
CPU_MED = CPU_MEDIUM
assert isinstance(CPU_MEDIUM, int)

CPU_HIGH = config.get(
    OPTIONS.cpu_high.name, OPTIONS.cpu_high.default
)
assert isinstance(CPU_HIGH, int)

CPU_MAX = config.get(
    OPTIONS.cpu_max.name, OPTIONS.cpu_max.default
)
assert isinstance(CPU_MAX, int)

# Memory limits
# First, the actual variables are set to None
# to indicate that the config info still needs
# to be parsed and turned into an integer.
# Reasoning:
# This is a design decision because the config
# settings here should be simple, i.e., consist
# essentially of value assignments. Parsing the
# potential suffix and converting the string into
# the appropriately scaled number/integer requires
# more complex logic that is implemented in the
# pyutils branch of the 'commons' modules. The pyutils
# will only be included later because they have
# many more dependencies to the parameters
# specified in the 'settings' branch. Hence, it is
# not reasonable to first include pyutils and then
# settings to realize the parsing/conversion right here.
MEM_MAX = None
MEM_COMMON = None

_MEM_MAX_CONFIG = config.get(
    OPTIONS.mem_max.name, OPTIONS.mem_max.default
)

_MEM_COMMON_CONFIG = config.get(
    OPTIONS.mem_common.name, OPTIONS.mem_common.default
)

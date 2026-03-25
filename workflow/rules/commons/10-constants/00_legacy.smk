"""Minimalistic module to immediately
determine if the workflow is executed
in Snakemake legacy mode (v7 or older)
or in more recent versions.
"""

import snakemake
import semver

# this is the critical variable to
# determine whether or not the workflow
# is executed with old/legacy versions
# of Snakemake (<= 7) or recent ones
# --- where needed, code downstream of this
# must check this condition like this:
#
# if SNAKEMAKE_LEGACY_RUN:
#   do snakemake/old stuff
# else:
#   do snakemake/new stuff
_SNAKEMAKE_LEGACY_THRESHOLD = 8
_SNAKEMAKE_VERSION = semver.parse_version_info(snakemake.__version__)
SNAKEMAKE_LEGACY_RUN = _SNAKEMAKE_VERSION.major < _SNAKEMAKE_LEGACY_THRESHOLD

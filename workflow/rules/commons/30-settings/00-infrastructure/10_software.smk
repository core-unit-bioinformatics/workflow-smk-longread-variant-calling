"""Module containing global infrastructure
settings pertaining to the software
environment that is assumed to be under
the control of an IT department / the local
system administrators (= on managed infrastructure).
Typically, the parameters in this module only
need a non-default configuration if the
system-wide software configuration is not
fully supporting all features regarding,
e.g., software deployment that Snakemake
has built-in.
"""

# needed if Singularity/Apptainer is not
# in $PATH by default on a managed system.
ENV_MODULE_SINGULARITY = config.get(
    OPTIONS.env_singularity.name, OPTIONS.env_singularity.default
)

# this is to prep dropping Singularity support;
# expectation is the switch from Singularity to Apptainer
# takes some time on managed infrastructure for reasons
# of backwards compatibility
ENV_MODULE_APPTAINER = config.get(
    OPTIONS.env_singularity.name, OPTIONS.env_singularity.default
)

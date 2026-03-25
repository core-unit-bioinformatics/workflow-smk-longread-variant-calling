"""Module containing global settings
that affect Snakemake's runtime behavior,
in particular how software deployment works.
All settings in this module are shorthands
for Snakemake command line / profile options
except for:

    RUN_IN_DEV_MODE
    RUN_IN_TEST_MODE
        see description below for details

Some settings are captured via the Snakemake
invocation call, see module
commons::10-constants::10_capture_cli
for details (how and why)
"""

VERBOSE = None
DEBUG = None
DRYRUN = None
USE_CONDA = None
USE_SINGULARITY = None
USE_APPTAINER = None
USE_CONTAINER = None
USE_ENV_MODULES = None

RUN_IN_DEV_MODE = None
RUN_IN_TEST_MODE = None

# this info is captured from the invocation
# call for both legacy and non-legacy
# see: commons::10-constants::10_capture_cli
DEBUG = _SMK_CLI_CAPTURE_DEBUG

if SNAKEMAKE_LEGACY_RUN:
    # in legacy Snakemake versions,
    # direct access to all these attributes
    # via the workflow object from global namespace
    VERBOSE = workflow.verbose
    USE_CONDA = workflow.use_conda
    USE_SINGULARITY = workflow.use_singularity
    USE_APPTAINER = USE_SINGULARITY
    USE_CONTAINER = USE_APPTAINER
    USE_ENV_MODULES = workflow.use_env_modules

    # special case: the --dry-run option is not accessible
    # as, e.g., an attribute of the workflow object and has
    # to be extracted from the invocation call in the legacy
    # version of Snakemake.
    # see: commons::10-constants::10_capture_cli
    DRYRUN = _SMK_CLI_CAPTURE_DRYRUN

else:
    # simple access to the active workflow
    # via the respective object from the global
    # namespace
    VERBOSE = workflow.output_settings.verbose
    DRYRUN = workflow.output_settings.dryrun
    _deployment_method_conda = api.DeploymentMethod["CONDA"]
    _deployment_method_apptainer = api.DeploymentMethod["APPTAINER"]
    _deployment_method_env_modules = api.DeploymentMethod["ENV_MODULES"]
    USE_CONDA = _deployment_method_conda in workflow.deployment_settings.deployment_method
    USE_SINGULARITY = _deployment_method_apptainer in workflow.deployment_settings.deployment_method
    USE_APPTAINER = USE_SINGULARITY
    USE_CONTAINER = USE_APPTAINER
    USE_ENV_MODULES = _deployment_method_env_modules in workflow.deployment_settings.deployment_method


assert isinstance(VERBOSE, bool)
assert isinstance(DEBUG, bool)
assert isinstance(DRYRUN, bool)
assert isinstance(USE_CONDA, bool)
assert isinstance(USE_SINGULARITY, bool)
assert isinstance(USE_APPTAINER, bool)
assert isinstance(USE_ENV_MODULES, bool)


# Special case: only constant set via the command line
# that is not a Snakemake CLI parameter, but has been
# introduced with this workflow template.
# === Why do we need this?
# If a workflow is executed in development mode, we
# can safely ignore, e.g., that some of the default
# output directories are missing
RUN_IN_DEV_MODE = config.get(
    OPTIONS.devmode.name, OPTIONS.devmode.default
)
assert isinstance(RUN_IN_DEV_MODE, bool)

# check if the workflow template tests are executed
RUN_IN_TEST_MODE = any(
    target in ["run_tests", "run_tests_no_manifest"]
    for target in _SMK_CLI_CAPTURE_TARGETS
)
assert isinstance(RUN_IN_TEST_MODE, bool)

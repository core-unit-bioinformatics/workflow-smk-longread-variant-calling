"""This file contains all
includes of the 'commons'
module branch of the template.
It must never contain or include
anything else.
"""

# At the moment, this module
# is empty/contains just a
# placefolder; this will be
# updated to implement a small
# "doc-gen" class that enables
# template/workflow developers
# to record documentation in-place
# where it is otherwise not supported
# out-of-the-box
include: "05_docgen.smk"

# Constants such as standard
# folders inside the working
# directory, and information
# directly captured by re-parsing
# the Snakemake command line
include: "10-constants/00_legacy.smk"
include: "10-constants/10_capture_cli.smk"
include: "10-constants/20_const_structs.smk"
include: "10-constants/30_units.smk"

# Description and default
# values for all supported
# generic config options
include: "20_config_options.smk"

# Module defining a large number
# of global variables such as
# relative paths to be used
# throughout the workflow
include: "30-settings/00-infrastructure/00_hardware.smk"
include: "30-settings/00-infrastructure/10_software.smk"
include: "30-settings/10-runtime/00_snakemake.smk"
include: "30-settings/20-environment/05_paths.smk"
include: "30-settings/20-environment/10_file_constants.smk"
include: "30-settings/20-environment/20_accounting.smk"
include: "30-settings/20-environment/30_ref_container.smk"

# Module containing Python-only
# helper functions grouped by context.
# *_simple modules contain helper functions
# that may be of use in generic worflow contexts
# (i.e., for template users/workflow devs)
# *_template modules contain helper functions
# that implement core functionality of the
# template 'commons' module branch
include: "40-pyutils/05_simple_get.smk"
include: "40-pyutils/10_simple_convert.smk"
include: "40-pyutils/15_simple_logging.smk"
include: "40-pyutils/20_simple_fs.smk"
include: "40-pyutils/75_template_git.smk"
include: "40-pyutils/80_template_refcon.smk"
include: "40-pyutils/85_template_accounting.smk"
include: "40-pyutils/90_template_staging.smk"

# Module containing Snakemake rules
# to accomplish template-specific
# tasks such as creating the
# manifest file, generating the
# help texts and dealing with
# CUBI-style reference containers
include: "50-smkutils/10_file_constants.smk"
include: "50-smkutils/80_template_refcon.smk"
include: "50-smkutils/85_template_accounting.smk"
include: "50-smkutils/90_help_docs.smk"

# New small module branch containing
# rules to test mostly the functionality
# of the Python helper/utility functions
# in '40-pyutils/'
# Naturally, this must be the last module
# branch to be included.
# Include only if needed / test run is
# intended to minimize potential name clashes
if RUN_IN_TEST_MODE:
    include: "99-testing/05_pythonics.smk"
    include: "99-testing/10_file_io.smk"
    include: "99-testing/20_refcon.smk"
    include: "99-testing/99_aggregate.smk"


# Module performing state/env-altering
# operations before Snakemake starts its
# actual work
include: "90_staging.smk"

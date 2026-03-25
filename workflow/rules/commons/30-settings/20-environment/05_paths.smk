"""Module containing global variables
that expose fully resolved paths as
shorthand for the user. Using these
variables avoids common typo mistakes
such as switching from 'log/' to 'logs/'
somewhere in the workflow.
"""


### === IMPORTANT === ###
### the following set of paths describe files
### and folders relative to the repository
### of the workflow, e.g., the location
### of the Snakefile itself. Except for the
### path to the conda environment files
### (variable DIR_ENVS), these variables
### are typically not needed to implement
### a new workflow (user-perspective).
###
### The paths to specify input/output files
### in a workflow are set in the second half
### of this file.

# start with all paths describing the repository
DIR_SNAKEFILE = pathlib.Path(workflow.basedir).resolve(strict=True)
assert DIR_SNAKEFILE.name == "workflow"  # by convention/best practices
PATH_SNAKEFILE = pathlib.Path(workflow.main_snakefile).resolve(strict=True)
NAME_SNAKEFILE = PATH_SNAKEFILE.stem
assert DIR_SNAKEFILE.samefile(PATH_SNAKEFILE.parent)

# If the name of the snakefile is not "Snakefile" and the
# developer has not set the devmode option, print a hint
# to help diagnose path resolution errors
if RUN_IN_TEST_MODE and not RUN_IN_DEV_MODE:
    hint_msg = (
        "\nDEV HINT:\n"
        "Looks like you are executing the builtin"
        " template testing module,"
        " but you did not set the config option:\n"
        " '--config devmode=True'\n"
        "This may lead to FileNotFoundErrors for the"
        " subfolders expected to exist"
        " (proc/, results/ and so on) in the working directory.\n"
        "This may of course be intended to actually test"
        " for passes/fails related to the assumed default"
        " directory structure.\nThat is, the code logic of"
        " the template will not interfere with the process,"
        " you have to know for yourself what you want to"
        " achieve here.\n\n"
    )
    sys.stderr.write(hint_msg)

DIR_REPOSITORY = DIR_SNAKEFILE.parent
DIR_REPO = DIR_REPOSITORY

# use of scripts is optional, but testing script is part
# of the template (= must resolve)
DIR_SCRIPTS = DIR_SNAKEFILE.joinpath(
    CONST_DIRS.scripts
).resolve(strict=True)

# must also exist because of the template's
# development and execution conda env/yaml
# files
DIR_ENVS = DIR_SNAKEFILE.joinpath(
    CONST_DIRS.envs
).resolve(strict=True)


### === IMPORTANT === ###
### the following set of paths describe files
### and folders relative to the specified
### Snakemake working directory, i.e.
### relative to 'snakemake -d <wd>'
### These variables are typically used
### to specify input/output paths in
### Snakemake rules.

# all paths describing the working directory
# plus default subfolders
DIR_WORKING = pathlib.Path(workflow.workdir_init).resolve(strict=True)
WORKDIR = DIR_WORKING

# if the workflow is executed in development mode,
# the default paths underneath the working directory
# may not exist and that is ok
WD_PATHS_MUST_RESOLVE = not RUN_IN_DEV_MODE

WD_ABSPATH_PROCESSING = CONST_DIRS.proc.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_PROCESSING = WD_ABSPATH_PROCESSING.relative_to(DIR_WORKING)
# recommended shorthand for 'processing' folder:
DIR_PROC = WD_RELPATH_PROCESSING

WD_ABSPATH_RESULTS = CONST_DIRS.results.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_RESULTS = WD_ABSPATH_RESULTS.relative_to(DIR_WORKING)
# recommended shorthand for 'results' folder:
DIR_RES = WD_RELPATH_RESULTS

WD_ABSPATH_LOG = CONST_DIRS.log.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_LOG = WD_ABSPATH_LOG.relative_to(DIR_WORKING)
# recommended shorthand for 'log' folder:
DIR_LOG = WD_RELPATH_LOG

WD_ABSPATH_RSRC = CONST_DIRS.rsrc.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_RSRC = WD_ABSPATH_RSRC.relative_to(DIR_WORKING)
# recommended shorthand for 'rsrc' [benchmark] folder:
DIR_RSRC = WD_RELPATH_RSRC
DIR_BENCHMARK = DIR_RSRC

WD_ABSPATH_CLUSTERLOG_OUT = CONST_DIRS.cluster_log_out.resolve(
    strict=WD_PATHS_MUST_RESOLVE
)
WD_RELPATH_CLUSTERLOG_OUT = WD_ABSPATH_CLUSTERLOG_OUT.relative_to(DIR_WORKING)
DIR_CLUSTERLOG_OUT = WD_RELPATH_CLUSTERLOG_OUT

WD_ABSPATH_CLUSTERLOG_ERR = CONST_DIRS.cluster_log_err.resolve(
    strict=WD_PATHS_MUST_RESOLVE
)
WD_RELPATH_CLUSTERLOG_ERR = WD_ABSPATH_CLUSTERLOG_ERR.relative_to(DIR_WORKING)
DIR_CLUSTERLOG_ERR = WD_RELPATH_CLUSTERLOG_ERR

WD_ABSPATH_GLOBAL_REF = CONST_DIRS.global_ref.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_GLOBAL_REF = WD_ABSPATH_GLOBAL_REF.relative_to(DIR_WORKING)
# recommended shorthand for 'global_ref' [external/global references] folder:
DIR_GLOBAL_REF = WD_RELPATH_GLOBAL_REF

WD_ABSPATH_LOCAL_REF = CONST_DIRS.local_ref.resolve(strict=WD_PATHS_MUST_RESOLVE)
WD_RELPATH_LOCAL_REF = WD_ABSPATH_LOCAL_REF.relative_to(DIR_WORKING)
# recommended shorthand for 'local_ref' [internal references] folder:
DIR_LOCAL_REF = WD_RELPATH_LOCAL_REF

"""This module re-parses and thus
captures the invocation command line
of Snakemake to extract a very small
number of pieces of information.

Because of the below caveat, this
is the only module of the template
that also implements some Python helper
functions (w/o any dependencies to the
rest of the template, of course).

This module must stay as small as possible!

IMPORTANT / CAVEAT
There is a non-zero (actually,
quite a large) chance that
the functionality in this module
is a "clunky" workaround that
was deemed necessary because
Snakemake's documentation is not
always detailed enough to see
how to obtain these values in
a more direct way.
(in particular for recent Snakemake
versions, see, e.g. snakemake/issues/2792)
"""

import enum
import os
import pathlib
import pickle


# These are temporary capture variables
# that are partially used in the correct location
# commons::30-settings::10-runtime::00_snakemake.smk
# If this module is ever dropped entirely because
# obtaining this information is implemented
# in more direct ways, all of the below must
# be moved into the appropriate sub-module.
_SMK_CLI_CAPTURE_ARGS = None
_SMK_CLI_CAPTURE_CACHE_FILE = None
_SMK_CLI_MAIN_PROCESS = None
_SMK_CLI_CAPTURE_DRYRUN = None
_SMK_CLI_CAPTURE_DEBUG = None
_SMK_CLI_CAPTURE_TARGETS = None
_SMK_CLI_IS_MAIN_PROCESS = None


def _parse_snakemake_invocation_command_line():
    """This function mimicks the respective code block
    in snakemake.__init__.py::main() (first few lines)
    """
    try:
        from snakemake import get_argument_parser as get_smk_cli_parser
    except ImportError:
        # this is already known at this point
        assert not SNAKEMAKE_LEGACY_RUN
        from snakemake.cli import get_argument_parser as get_smk_cli_parser

    smk_cli_parser = get_smk_cli_parser()
    args, _ = smk_cli_parser.parse_known_intermixed_args(sys.argv)

    return args


def _find_cli_cache_file(is_main_process):
    """The process group ID is stable in
    case of job execution in children of
    the main process. If not (e.g., cluster),
    just find the most recent file.
    """
    pgid = os.getpgid(0)
    default_path = pathlib.Path(
        f"{pgid}.cli.pck"
    )
    if default_path.is_file():
        cache_file = default_path
    elif is_main_process:
        # to be created then
        cache_file = default_path
    else:
        # must exist, created by main
        cli_files = sorted(
            pathlib.Path(".").glob("*.cli.pck"),
            key=lambda fp: os.path.getmtime(fp),
            reverse=True
        )
        # we select the most recent one, sorted
        # above by mtime from higher (younger)
        # to lower (older)
        # NB: this naturally fails is no cli
        # cache file has been created, which is
        # an expected raise
        cache_file = cli_files[0]

    return cache_file


def _dump_cli_cache_file(cache_file, smk_args):
    """Snakemake execution subprocesses are
    initialized w/ a modified command line,
    so we preserve the original invocation
    command line to access info such as the
    tests being executed (necessary for
    correct module includes in legacy runs).
    """
    with open(cache_file, "wb") as cache:
        pickle.dump(smk_args, cache)
    return


def _read_cli_cache_file(cache_file):
    """
    """
    with open(_SMK_CLI_CAPTURE_CACHE_FILE, "rb") as cache:
        smk_args = pickle.load(cache)
    return smk_args


_SMK_CLI_CAPTURE_ARGS = _parse_snakemake_invocation_command_line()
if SNAKEMAKE_LEGACY_RUN:
    assert isinstance(_SMK_CLI_CAPTURE_ARGS.mode, int)
    _SMK_CLI_IS_MAIN_PROCESS = _SMK_CLI_CAPTURE_ARGS.mode < 1
else:
    assert isinstance(_SMK_CLI_CAPTURE_ARGS.mode, enum.Enum)
    _SMK_CLI_IS_MAIN_PROCESS = _SMK_CLI_CAPTURE_ARGS.mode.value < 1

_SMK_CLI_CAPTURE_CACHE_FILE = _find_cli_cache_file(_SMK_CLI_IS_MAIN_PROCESS)
if _SMK_CLI_IS_MAIN_PROCESS:
    _dump_cli_cache_file(_SMK_CLI_CAPTURE_CACHE_FILE, _SMK_CLI_CAPTURE_ARGS)
else:
    _SMK_CLI_CAPTURE_ARGS = _read_cli_cache_file(_SMK_CLI_CAPTURE_CACHE_FILE)


_SMK_CLI_CAPTURE_DEBUG = _SMK_CLI_CAPTURE_ARGS.debug

if SNAKEMAKE_LEGACY_RUN:
    # Why not for both legacy and non-legacy?
    # Because it's not the right thing to do here;
    # the 'dry run' information is easily available
    # for recent versions of Snakemake as a member
    # of the OutputSettings()
    _SMK_CLI_CAPTURE_DRYRUN = _SMK_CLI_CAPTURE_ARGS.dryrun

if SNAKEMAKE_LEGACY_RUN:
    _SMK_CLI_CAPTURE_TARGETS = _SMK_CLI_CAPTURE_ARGS.target
else:
    # note the different spelling: >s<
    _SMK_CLI_CAPTURE_TARGETS = _SMK_CLI_CAPTURE_ARGS.targets

# Known by introspection, first element points to snakemake executable
assert len(_SMK_CLI_CAPTURE_TARGETS) > 0
assert "snakemake" in _SMK_CLI_CAPTURE_TARGETS[0]

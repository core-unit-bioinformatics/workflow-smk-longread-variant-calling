"""This module serves as single source
of truth regarding the constant paths
listed below (special files and directories).
Changing this module affects other code:
- init.py script
"""

import dataclasses
import pathlib


@dataclasses.dataclass(frozen=True, eq=False)
class ConstDirectories:
    proc: pathlib.Path = pathlib.Path("proc")
    results: pathlib.Path = pathlib.Path("results")
    log: pathlib.Path = pathlib.Path("log")
    rsrc: pathlib.Path = pathlib.Path("rsrc")
    cluster_log_out: pathlib.Path = pathlib.Path("log", "cluster_jobs", "out")
    cluster_log_err: pathlib.Path = pathlib.Path("log", "cluster_jobs", "err")
    global_ref: pathlib.Path = pathlib.Path("global_ref")
    local_ref: pathlib.Path = pathlib.Path("local_ref")
    scripts: pathlib.Path = pathlib.Path("scripts")
    envs: pathlib.Path = pathlib.Path("envs")
    cache: pathlib.Path = pathlib.Path("proc", ".cache")
    accounting: pathlib.Path = pathlib.Path("proc", ".accounting")
    cache_refcon: pathlib.Path = pathlib.Path("proc", ".cache", "refcon")
    _no_init: tuple = ("scripts", "envs", "cache", "accounting", "cache_refcon")


CONST_DIRS = ConstDirectories()


@dataclasses.dataclass(frozen=True, eq=False)
class ConstFiles:
    refcon_cache: pathlib.Path = pathlib.Path(
        CONST_DIRS.cache_refcon, "refcon_manifests.cache"
    )
    account_inputs: pathlib.Path = pathlib.Path(CONST_DIRS.accounting, "inputs.listing")
    account_references: pathlib.Path = pathlib.Path(
        CONST_DIRS.accounting, "references.listing"
    )
    account_results: pathlib.Path = pathlib.Path(
        CONST_DIRS.accounting, "results.listing"
    )
    # Note that both manifest and config dump may be modified via the
    # "suffix" option and the path may thus differ at runtime.
    # Yes, yes, quite idiosyncratic, but nothing is really constant in
    # Python anyway ...
    manifest: pathlib.Path = pathlib.Path(CONST_DIRS.results, "manifest.tsv")
    config_dump: pathlib.Path = pathlib.Path(CONST_DIRS.results, "run_config.yaml")


CONST_FILES = ConstFiles()

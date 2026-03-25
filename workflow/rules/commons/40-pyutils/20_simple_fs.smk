"""Module implementing simple utility functions
to standardize basic file system operations
(copy/move etc.) intended to be called inside
a rule's run block.

Functions in this module should not introduce
technical dependencies outside of Python's
standard lib and default Linux/bash tools
for file copy/move operations.
"""

import pathlib
import subprocess


def rsync_f2d(source_file, target_dir):
    """
    Convenience function to 'rsync' a source
    file into a target directory (file name
    not changed) in a 'run' block of a
    Snakemake rule.
    """
    abs_source = pathlib.Path(source_file).resolve(strict=True)
    abs_target = pathlib.Path(target_dir).resolve(strict=False)
    abs_target.mkdir(parents=True, exist_ok=True)
    _rsync(str(abs_source), str(abs_target))
    return


def rsync_f2f(source_file, target_file):
    """
    Convenience function to 'rsync' a source
    file to a target location (copy file
    and change name) in a 'run' block of
    a Snakemake rule.
    """
    abs_source = pathlib.Path(source_file).resolve(strict=True)
    abs_target = pathlib.Path(target_file).resolve(strict=False)
    abs_target.parent.mkdir(parents=True, exist_ok=True)
    _rsync(str(abs_source), str(abs_target))
    return


def _rsync(source, target):
    """
    Abstract function realizing 'rsync' calls;
    do not call this function, use 'rsync_f2f'
    or 'rsync_f2d'.
    """
    cmd = ["rsync", "--quiet", "--checksum", source, target]
    try:
        _ = subprocess.check_call(cmd, shell=False)
    except subprocess.CalledProcessError as spe:
        logerr(f"rsync from '{source}' to '{target}' failed")
        raise spe
    return

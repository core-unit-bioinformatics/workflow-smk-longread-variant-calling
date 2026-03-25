"""
The staging module is the right place to trigger
code executions that affect the state/env
the workflow is executed in and that need
to be executed before Snakemake starts
its actual work.

This is the only "commons" module that is
dependent on the other "commons" modules.
In other words, here, the series of
"include:" statements in "00_commons.smk"
matters.

Functions called here should be written
in a very defensive manner, and always
be tested in some way via "snaketests.smk"
"""

# reset accounting files if requested
_reset_file_accounts()


# In case of a dry run, the cli capture
# cache file is not deleted because a
# dry run is not counted as a successful
# workflow execution (tested in v7.32).
# Hence, we are deleting this file here
# manually to avoid that the 'dry run'
# state is wrongfully propagated by
# reading from that cache file.
if DRYRUN:
    shell(f"rm -f {_SMK_CLI_CAPTURE_CACHE_FILE}")

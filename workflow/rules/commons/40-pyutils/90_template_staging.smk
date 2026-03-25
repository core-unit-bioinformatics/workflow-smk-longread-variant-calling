"""The staging module is the place to put
functions that should only be executed when
Snakemake is ready to run, i.e., all variables
have been initialized, all dry run data have
been collected and so on.

Additions to this module are only allowed
under the "if absolutely necessary" rule
and should not require any further interaction.

Remember to call all functions from this
module in commons::90_staging.smk
"""


def _reset_file_accounts():
    """
    In case erroneous entries in any of the
    file account prevent a proper execution
    of the pipeline, this function can be
    triggered by setting --config resetacc=True,
    and will delete the accounting files. Next,
    the workflow should be re-executed with
    `--dryrun` to recreate the accounting files.
    """
    if RESET_ACCOUNTING:
        for acc_name, acc_path in ACCOUNTING_FILES.items():
            if VERBOSE:
                logerr(f"Resetting accounting file {acc_name}")
            try:
                with open(acc_path, "w"):
                    pass
            except FileNotFoundError:
                pass
            # important: reset cached path IDs
            path_id_cache = pathlib.Path(acc_path).with_suffix(".paths.pck")
            try:
                with open(path_id_cache, "w"):
                    pass
            except FileNotFoundError:
                pass

    for acc_file_path in ACCOUNTING_FILES.values():
        acc_path = pathlib.Path(acc_file_path).parent
        acc_path.mkdir(exist_ok=True, parents=True)

    return

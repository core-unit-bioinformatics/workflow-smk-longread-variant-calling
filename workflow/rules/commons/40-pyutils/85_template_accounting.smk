"""This module implements the functions to
create the workflow manifest aka provides the
necessary utilities to perform the file accounting.
To non-template (workflow) developers, the
register_* functions are the only relevant ones
that must be used as parameters in the Snakemake
rules where input, output or reference files
shall be recorded (be part of the manifest).
"""

import hashlib
import pathlib
import pickle

# locking capability potentially
# needed for file accounting;
# in the current implementation
# (only update during dry run)
# probably not needed
import portalocker


def _add_file_to_account(accounting_file, fmt_records):
    """
    Add registered files to the respective accounting file;
    do not duplicate records by caching the set of known path
    ids in a separate (pickle dump) file.
    """
    lockfile = pathlib.Path(accounting_file).with_suffix(".lock")
    path_id_file = pathlib.Path(accounting_file).with_suffix(".paths.pck")
    with portalocker.Lock(lockfile, "a", timeout=WAIT_ACC_LOCK_SECS) as lock:
        try:
            with open(path_id_file, "rb") as path_id_dump:
                known_path_ids = pickle.load(path_id_dump)
        except (FileNotFoundError, EOFError):
            known_path_ids = set()

        with open(accounting_file, "a") as account:
            for path_id, path_records in fmt_records:
                if path_id in known_path_ids:
                    continue
                _ = account.write(path_records)
                known_path_ids.add(path_id)

        with open(path_id_file, "wb") as path_id_dump:
            _ = pickle.dump(known_path_ids, path_id_dump)
    return


def _add_checksum_files(add_files, subfolder):
    """
    Augment each file registered for manifest inclusion
    with three additional files recording md5 and sha256
    checksums and the file size in bytes.
    """
    formatted_records = []
    for add_file in add_files:
        file_name = add_file.name
        abspath = str(add_file.resolve(strict=False))
        path_id = hashlib.md5(abspath.encode("utf-8")).hexdigest()
        md5_checksum = DIR_PROC.joinpath(
            ".accounting", "checksums", f"{subfolder}", f"{file_name}.{path_id}.md5"
        )
        sha256_checksum = DIR_PROC.joinpath(
            ".accounting", "checksums", f"{subfolder}", f"{file_name}.{path_id}.sha256"
        )
        size_file = DIR_PROC.joinpath(
            ".accounting", "file_sizes", f"{subfolder}", f"{file_name}.{path_id}.bytes"
        )
        formatted_records.append(
            (
                path_id,
                f"{add_file}\t{path_id}\t{subfolder}\tdata\n"
                + f"{md5_checksum}\t{path_id}\t{subfolder}\tchecksum\n"
                + f"{sha256_checksum}\t{path_id}\t{subfolder}\tchecksum\n"
                + f"{size_file}\t{path_id}\t{subfolder}\tsize\n",
            )
        )

    return formatted_records


def register_input(*args, allow_non_existing=False):
    """
    This register function has slightly
    different semantics because input files
    should always exist when the workflow starts.
    For special cases, a keyword-only argument can
    be set to accept non-existing files when the
    pipeline run starts and yet the files should
    be counted as input files. One of those
    special cases is the config dump, which is
    counted as part of the input (cannot run
    the workflow w/o config), but the dump
    is only created after execution.

    Note: the return value must be constant to
    avoid that Snakemake triggers a rerun b/c
    of a changed rule parameter
    """
    if DRYRUN:
        accounting_file = ACCOUNTING_FILES["inputs"]
        input_files = flatten_nested_paths(args)
        if not allow_non_existing:
            err_msg = ""
            for file_path in input_files:
                if not file_path.exists():
                    err_msg += (
                        f"register_input() -> input file does not exist: {file_path}\n"
                    )
                elif file_path.is_dir():
                    err_msg += f"Specified input is not a regular file: {file_path}\n"
                else:
                    pass
            if err_msg:
                err_msg = f"\nINPUT ERROR:\n{err_msg}"
                logerr(err_msg)
                raise RuntimeError("Bad input files detected (see above)")
        fmt_records = _add_checksum_files(input_files, "inputs")
        _add_file_to_account(accounting_file, fmt_records)
    return None


def register_reference(*args):
    """
    See note in "register_input" regarding
    return value; same here.
    """
    if DRYRUN:
        accounting_file = ACCOUNTING_FILES["references"]
        reference_files = flatten_nested_paths(args)
        fmt_records = _add_checksum_files(reference_files, "references")
        _add_file_to_account(accounting_file, fmt_records)
    return None


def register_result(*args):
    """
    See note in "register_input" regarding
    return value; same here.
    """
    if DRYRUN:
        accounting_file = ACCOUNTING_FILES["results"]
        result_files = flatten_nested_paths(args)
        fmt_records = _add_checksum_files(result_files, "results")
        _add_file_to_account(accounting_file, fmt_records)
    return None


def load_accounting_information(wildcards):
    """
    This function loads the file paths from all
    three accounting files to force creation
    (relevant for checksum and size files).
    """
    created_files = []
    for account_name, account_file in ACCOUNTING_FILES.items():
        if VERBOSE:
            logerr(f"Loading file account {account_name}")
        try:
            with open(account_file, "r") as account:
                created_files.extend(
                    [l.split()[0] for l in account.readlines() if l.strip()]
                )
            if VERBOSE:
                logerr(f"Size of accounting file list: {len(created_files)}")
        except FileNotFoundError:
            if VERBOSE:
                warn_msg = f"Accounting file does not exist (yet): {account_file}\n"
                warn_msg += "Please RERUN the workflow in DRY RUN MODE to create the file accounts!"
                logerr(warn_msg)
    return sorted(created_files)


def load_file_by_path_id(wildcards):
    """ """
    account_type = wildcards.file_type
    assert account_type in ACCOUNTING_FILES
    account_file = ACCOUNTING_FILES[account_type]

    req_path_id = wildcards.path_id
    req_file = None
    with open(account_file, "r") as account:
        for line in account:
            # note here: the data file is always
            # first in the list of four entries
            file_path, file_path_id = line.split()[:2]
            if file_path_id != req_path_id:
                continue
            req_file = file_path
            break
    if req_file is None:
        logerr(
            f"Failed loading file with path ID {req_path_id} from accounting file {account_file}"
        )
        raise FileNotFoundError(
            f"Missing file: {wildcards.file_type} / {wildcards.file_name}"
        )

    return req_file


def _load_data_line(file_path):
    with open(file_path, "r") as dump:
        content = dump.readline().strip().split()[0]
    return content


def process_accounting_record(line):
    """ """
    path, path_id, file_category, record_type = line.strip().split()
    if record_type == "data":
        record = {
            "file_path": path,
            "file_name": pathlib.Path(path).name,
            "file_category": file_category,
            "path_id": path_id,
        }
    elif record_type == "checksum":
        checksum_type = path.rsplit(".", 1)[-1]
        record = {
            f"file_checksum_{checksum_type}": _load_data_line(path),
            "path_id": path_id,
        }
    elif record_type == "size":
        size_unit = path.rsplit(".", 1)[-1]
        record = {
            f"file_size_{size_unit}": int(_load_data_line(path)),
            "path_id": path_id,
        }
    return path_id, record

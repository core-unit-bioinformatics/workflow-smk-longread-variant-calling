"""Module containing settings
for internal file accounting,
which is the basis for creating
the manifest output file.
"""

# For file-based locking of accounting files.
# Expert-only option in case accounting outside
# of dry runs becomes necessary
# TODO - ?
# 2025-07: seems unlikely that file accounting
# needs to happen in the foreseeable
# future; this dependency should be dropped
# in one of the next major updates.
WAIT_ACC_LOCK_SECS = config.get(
    OPTIONS.acclock.name, OPTIONS.acclock.default
)

# should the accounting files be reset/emptied?
RESET_ACCOUNTING = config.get(OPTIONS.resetacc.name, OPTIONS.resetacc.default)
assert isinstance(RESET_ACCOUNTING, bool)

# this is kept for backward-compatibility, but
# could be replaced with something more elegant
# making use of the new "10_constants" module
# TODO - ?
# 2025-07 somewhat more elegant to make the accounting
# directly available instead of the key-based lookup,
# but not a pressing issue.
# Changes would affect commons::pyutils and commons::smkutils
ACCOUNTING_FILES = {
    CONST_FILES.account_inputs.stem: CONST_FILES.account_inputs,
    CONST_FILES.account_references.stem: CONST_FILES.account_references,
    CONST_FILES.account_results.stem: CONST_FILES.account_results,
}

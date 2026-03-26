
WORKFLOW_OUTPUT = []

WORKFLOW_OUTPUT.extend(
    rules.test_all_pythonics.input
)

WORKFLOW_OUTPUT.extend(
    rules.test_all_file_io.input
)


if USE_REFERENCE_CONTAINER:
    WORKFLOW_OUTPUT.append(
        rules.test_refcon_functionality.output[0]
    )


# NB: remember that this is only executed
# in a test run of the workflow
if VERBOSE or DEBUG:
    WORKFLOW_OUTPUT = sorted(WORKFLOW_OUTPUT)
    for filepath in WORKFLOW_OUTPUT:
        logerr(f"Creating test file: {filepath}")

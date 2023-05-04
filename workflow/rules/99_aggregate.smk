"""
Use this module to extend the default
workflow output (a list of target files)
per sub-module.
The WORKFLOW_OUTPUT list is referenced
in the main Snakefile
"""

WORKFLOW_OUTPUT = []

WORKFLOW_OUTPUT.extend(ALIGN_HIFI_OUTPUT)
WORKFLOW_OUTPUT.extend(ALIGN_ONT_OUTPUT)

WORKFLOW_OUTPUT.extend(COV_HIFI_GENOME)
WORKFLOW_OUTPUT.extend(COV_HIFI_ROI)

WORKFLOW_OUTPUT.extend(COV_ONT_GENOME)
WORKFLOW_OUTPUT.extend(COV_ONT_ROI)

WORKFLOW_OUTPUT.extend(CALL_HIFI_SHORT_OUTPUT)
WORKFLOW_OUTPUT.extend(CALL_HIFI_SV_OUTPUT)

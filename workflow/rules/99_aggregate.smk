"""
Use this module to extend the default
workflow output (a list of target files)
per sub-module.
The WORKFLOW_OUTPUT list is referenced
in the main Snakefile
"""

WORKFLOW_OUTPUT = []
# Example for extending the output
# with output from another module
# (remember to include that module
# in 00_modules.smk):
# WORKFLOW_OUTPUT.extend(MODULE_OUTPUT)

"""
Aggregate all
alignment jobs
"""

ALIGN_HIFI_OUTPUT = []
ALIGN_ONT_OUTPUT = []

if HIFI_ALIGNER_WILDCARDS:
    ALIGN_HIFI_OUTPUT.extend(
        rules.run_all_hifi_align.input
    )

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

    if USER_ROI_FILE_WILDCARDS:
        ALIGN_HIFI_OUTPUT.extend(
            rules.run_all_extract_roi_hifi_alignment_subset.input
        )

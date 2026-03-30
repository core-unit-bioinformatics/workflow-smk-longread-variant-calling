"""
Aggregate all
alignment jobs
"""

ALIGN_HIFI_OUTPUT = []
ALIGN_ONT_OUTPUT = []

if HIFI_SAMPLES and HIFI_ALIGNER_WILDCARDS:
    ALIGN_HIFI_OUTPUT.extend(
        rules.run_all_hifi_align.input
    )

    if USER_ROI_FILE_WILDCARDS:
        ALIGN_HIFI_OUTPUT.extend(
            rules.run_all_extract_roi_hifi_alignment_subset.input
        )

if ONT_SAMPLES and ONT_ALIGNER_WILDCARDS:
    ALIGN_ONT_OUTPUT.extend(
        rules.run_all_ont_align.input
    )

    if USER_ROI_FILE_WILDCARDS:
        ALIGN_ONT_OUTPUT.extend(
            rules.run_all_extract_roi_ont_alignment_subset.input
        )

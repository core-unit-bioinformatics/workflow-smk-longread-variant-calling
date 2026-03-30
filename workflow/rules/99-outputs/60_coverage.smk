"""
Aggregate all
read depth jobs
"""

COV_HIFI_GENOME = []
COV_HIFI_ROI = []

COV_ONT_GENOME = []
COV_ONT_ROI = []


if HIFI_SAMPLES:
    COV_HIFI_GENOME.extend(
        rules.compute_genome_hifi_read_depth.input.md_ok
    )
    COV_HIFI_GENOME.extend(
        rules.plot_agg_window_hifi_read_depth.input.pdf
    )

    if USER_ROI_FILE_WILDCARDS:
        COV_HIFI_ROI.extend(
            rules.compute_roi_hifi_read_depth.input.md_ok
        )

if ONT_SAMPLES:
    COV_ONT_GENOME.extend(
        rules.compute_genome_ont_read_depth.input.md_ok
    )
    COV_ONT_GENOME.extend(
        rules.plot_agg_window_ont_read_depth.input.pdf
    )

    if USER_ROI_FILE_WILDCARDS:
        COV_ONT_ROI.extend(
            rules.compute_roi_ont_read_depth.input.md_ok
        )

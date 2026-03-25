##########################
### SETTINGS FOR MOSDEPTH
##########################

MOSDEPTH_QUANTIZE_STEPS = config.get(
    "mosdepth_quantize_steps", [0, 1, 5, 10, 15]
)
MOSDEPTH_QUANTIZE_NAMES = config.get(
    "mosdepth_quantize_names",
    ["NO_COV", "LOW_COV", "CALLABLE", "GOOD_COV", "HIGH_COV"]
)
MOSDEPTH_WINDOW_SIZE = config.get("mosdepth_window_size", 10000)
assert isinstance(MOSDEPTH_WINDOW_SIZE, int)

MOSDEPTH_COV_THRESHOLDS = config.get(
    "mosdepth_cov_thresholds", [0, 1, 5, 10, 15]
)
assert isinstance(MOSDEPTH_COV_THRESHOLDS, list)
assert all(isinstance(v, int) for v in MOSDEPTH_COV_THRESHOLDS)

MOSDEPTH_MIN_MAPQ = config.get(
    "mosdepth_min_mapq", [0, 20]
)
assert all(isinstance(v, int) for v in MOSDEPTH_MIN_MAPQ)

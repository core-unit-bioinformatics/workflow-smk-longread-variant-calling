import sys

###########################################
### SETTINGS FOR SNIFFLES2 ONLY
###########################################

RUN_SNIFFLES_MOSAIC_MODE = config.get("run_sniffles_mosaic_mode", False)
assert isinstance(RUN_SNIFFLES_MOSAIC_MODE, bool)

if RUN_SNIFFLES_MOSAIC_MODE:
    sys.stderr.write("\nSniffles mosaic mode is currently not supported - reverting to FALSE\n")
    RUN_SNIFFLES_MOSAIC_MODE = False

RUN_SNIFFLES_MULTISAMPLE_MODE = config.get("run_sniffles_multisample_mode", False)
assert isinstance(RUN_SNIFFLES_MULTISAMPLE_MODE, bool)
SNIFFLES_MULTISAMPLE_SETS = config.get("sniffles_multisample_sets", dict())

import sys

###########################################
### SETTINGS FOR HIFI SV CALLING TOOLCHAIN
### THIS IS A GLOBAL CONTROL SWITCH/SETTING
###########################################

HIFI_SV_CALLER_NAME_MAPPING = {
    "sniffles": "sniffles",
}

RUN_HIFI_SV_CALLING_TOOLCHAIN = config.get("run_hifi_sv_toolchain", [])
if not RUN_HIFI_SV_CALLING_TOOLCHAIN and VERBOSE:
    sys.stderr.write("Warning: no HiFi SV calling toolchain configured to run.")

HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS = []

for toolchain in RUN_HIFI_SV_CALLING_TOOLCHAIN:
    aligner, caller = toolchain.split(",")
    wildcard_aln = HIFI_ALIGNER_NAME_MAPPING[aligner.strip().lower()]
    wildcard_call = HIFI_SV_CALLER_NAME_MAPPING[caller.strip().lower()]
    HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS.append(
        f"{wildcard_aln}-{wildcard_call}"
    )
    ### SPECIAL CASE FOR SNIFFLES - ADD MOSAIC MODE?
    if RUN_SNIFFLES_MOSAIC_MODE and wildcard_call == "sniffles":
        HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS.append(
            f"{wildcard_aln}-{wildcard_call}.mosaic"
        )
    ALIGNER_FOR_CALLER[(wildcard_call, "hifi")].append(wildcard_aln)
	
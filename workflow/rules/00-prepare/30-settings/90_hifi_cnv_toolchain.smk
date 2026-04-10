import sys

###########################################
### SETTINGS FOR HIFI CNV CALLING TOOLCHAIN
### THIS IS A GLOBAL CONTROL SWITCH/SETTING
###########################################

HIFI_CNV_CALLER_NAME_MAPPING = {
    "hificnv": "pbcnv",  # sic!
    "pbcnv": "pbcnv",
}

RUN_HIFI_CNV_CALLING_TOOLCHAIN = config.get("run_hifi_cnv_toolchain", [])

# TODO hard-code this until full workaround is implemented
# for the case of missing noise/expect CN annotations (see tool module)
RUN_HIFI_CNV_CALLING_TOOLCHAIN = []
log_err("DEV NOTE: HiFi CNV calling deactivated in 00-prepare::30-settings::90_hifi_cnv_toolchain.smk")

if not RUN_HIFI_CNV_CALLING_TOOLCHAIN and VERBOSE:
    log_err("Warning: no HiFi CNV calling toolchain configured to run.")

HIFI_CNV_CALLING_TOOLCHAIN_WILDCARDS = []

for toolchain in RUN_HIFI_CNV_CALLING_TOOLCHAIN:
    aligner, caller = toolchain.split(",")
    wildcard_aln = HIFI_ALIGNER_NAME_MAPPING[aligner.strip().lower()]
    wildcard_call = HIFI_CNV_CALLER_NAME_MAPPING[caller.strip().lower()]
    HIFI_CNV_CALLING_TOOLCHAIN_WILDCARDS.append(
        f"{wildcard_aln}-{wildcard_call}"
    )

    ALIGNER_FOR_CALLER[(wildcard_call, "hifi")].append(wildcard_aln)

HIFI_CNV_FOLDCHANGE_SCALE_FACTORS = config.get("hifi_cnv_foldchange_scale_factors", dict())

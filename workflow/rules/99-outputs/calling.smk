"""
Aggregate all
variant calling jobs
"""

CALL_HIFI_SHORT_OUTPUT = []
CALL_HIFI_SV_OUTPUT = []

if HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS:
    CALL_HIFI_SHORT_OUTPUT.extend(
        rules.run_concat_hifi_short_callsets.input.vcf
    )

if HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS:
    CALL_HIFI_SV_OUTPUT.extend(
        rules.run_hifi_finalize_sv_callsets.input.vcf
    )

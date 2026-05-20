"""
Aggregate all
variant calling jobs
"""

CALL_HIFI_SHORT_OUTPUT = []
CALL_HIFI_SV_OUTPUT = []
CALL_HIFI_CNV_OUTPUT = []
CALL_HIFI_TRIO_OUTPUT = []

if HIFI_SHORT_CALLING_TOOLCHAIN_WILDCARDS:
    CALL_HIFI_SHORT_OUTPUT.extend(rules.run_concat_hifi_short_callsets.input.vcf)
    CALL_HIFI_SHORT_OUTPUT.extend(rules.run_concat_hifi_short_callsets.input.txt_stats)
    CALL_HIFI_SHORT_OUTPUT.extend(rules.run_concat_hifi_short_callsets.input.tsv_stats)


if HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS:
    CALL_HIFI_SV_OUTPUT.extend(rules.run_hifi_finalize_sv_callsets.input.vcf)
    CALL_HIFI_SV_OUTPUT.extend(rules.run_hifi_finalize_sv_callsets.input.txt_stats)
    CALL_HIFI_SV_OUTPUT.extend(rules.run_hifi_finalize_sv_callsets.input.tsv_stats)


if HIFI_SV_CALLING_TOOLCHAIN_WILDCARDS and SAMPLE_PAIRS is not None:

    CALL_HIFI_SV_OUTPUT.extend(rules.run_hifi_finalize_sv_callsets_personalized.input)


if HIFI_CNV_CALLING_TOOLCHAIN_WILDCARDS:
    CALL_HIFI_CNV_OUTPUT.extend(rules.run_all_cnv_calling_pbcnv.input.cn_est)


if config["variant_calling_mode"] == "trio":

    # --- TRIO WHOLE-GENOME VCFs ---
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.trio_joint_vcfs
    )
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.trio_joint_txt_stats
    )

    # --- DUO WHOLE-GENOME VCFs ---
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.duo_joint_vcfs
    )
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.duo_joint_txt_stats
    )

    # --- MULTI-CHILD FAMILY WHOLE-GENOME VCFs ---
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.family_joint_vcfs
    )
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.family_joint_txt_stats
    )
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.trio_single_tsv_stats
    )
    CALL_HIFI_TRIO_OUTPUT.extend(
        rules.run_concat_hifi_trio_callsets.input.duo_single_tsv_stats
    )

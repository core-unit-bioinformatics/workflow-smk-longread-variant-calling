"""
Use this module to list all includes
required for your pipeline - do not
add your pipeline-specific modules
to "commons/00_commons.smk"
"""

include: "00-prepare/10_known_input.smk"
include: "00-prepare/20_sample_table.smk"
include: "00-prepare/30-settings/10_runtime.smk"
include: "00-prepare/30-settings/20_samtools.smk"
include: "00-prepare/30-settings/30_mosdepth.smk"
include: "00-prepare/30-settings/40_hifi_aligners.smk"
include: "00-prepare/30-settings/50_ont_aligners.smk"
include: "00-prepare/30-settings/60_hifi_callers.smk"
include: "00-prepare/30-settings/70_sniffles2.smk"
include: "00-prepare/30-settings/80_hifi_sv_toolchain.smk"
include: "00-prepare/30-settings/90_hifi_cnv_toolchain.smk"

include: "10-align/00_pyutils.smk"
include: "10-align/30_hifi.smk"
include: "10-align/60_ont.smk"

include: "15-genotype/30_pangenie_prep.smk"
include: "15-genotype/60_pangenie_type.smk"
include: "15-genotype/90_merge_genotyped.smk"

include: "17-personal-ref/00_pyutils.smk"
include: "17-personal-ref/30_phasing.smk"
include: "17-personal-ref/60_patching.smk"

include: "20-postalign/30_process.smk"

include: "25-coverage/00_pyutils.smk"
include: "25-coverage/30_read_depth.smk"
include: "25-coverage/60_aggregate.smk"

include: "30-callshort/30_hifi.smk"

include: "40-callsv/00_pyutils.smk"
include: "40-callsv/30_hifi.smk"

include: "45-callcnv/00_pyutils.smk"
include: "45-callcnv/30_hifi.smk"
include: "45-callcnv/60_convert_pbcnv.smk"

include: "50-postcall/10_split.smk"
include: "50-postcall/20_concat.smk"
include: "50-postcall/30_compress.smk"
include: "50-postcall/50_stats.smk"

include: "70-plotting/30_read_depth.smk"

include: "80-subset/00_pyutils.smk"
include: "80-subset/30_alignments.smk"
include: "80-subset/60_callset.smk"

include: "99-outputs/30_align.smk"
include: "99-outputs/60_coverage.smk"
include: "99-outputs/90_calling.smk"

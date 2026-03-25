"""
Use this module to list all includes
required for your pipeline - do not
add your pipeline-specific modules
to "commons/00_commons.smk"
"""

include: "00-prepare/known_input.smk"
include: "00-prepare/sample_table.smk"
#include: "00-prepare/settings.smk"
include: "00-prepare/30-settings/10-main.smk"
include: "00-prepare/30-settings/20-samtools.smk"
include: "00-prepare/30-settings/30-mosdepth.smk"
include: "00-prepare/30-settings/40-hifi-aligners.smk"
include: "00-prepare/30-settings/50-ont-aligners.smk"
include: "00-prepare/30-settings/60-hifi-short-and-sv.smk"
include: "00-prepare/30-settings/70-sniffles2.smk"
include: "00-prepare/30-settings/80-hifi-sv-calling.smk"
include: "00-prepare/30-settings/90-hifi-cnv-calling.smk"

include: "10-align/pyutils.smk"
include: "10-align/hifi.smk"
include: "10-align/ont.smk"

include: "15-genotype/pangenie_prep.smk"
include: "15-genotype/pangenie_type.smk"
include: "15-genotype/merge_genotyped.smk"

include: "17-personal-ref/pyutils.smk"
include: "17-personal-ref/phasing.smk"
include: "17-personal-ref/patching.smk"

include: "20-postalign/process.smk"

include: "25-coverage/pyutils.smk"
include: "25-coverage/read_depth.smk"
include: "25-coverage/aggregate.smk"

include: "30-callshort/hifi.smk"

include: "40-callsv/pyutils.smk"
include: "40-callsv/hifi.smk"

include: "45-callcnv/pyutils.smk"
include: "45-callcnv/hifi.smk"
include: "45-callcnv/convert_pbcnv.smk"

include: "50-postcall/10_split.smk"
include: "50-postcall/20_concat.smk"
include: "50-postcall/30_compress.smk"
include: "50-postcall/50_stats.smk"

include: "70-plotting/read_depth.smk"

include: "80-subset/pyutils.smk"
include: "80-subset/alignments.smk"
include: "80-subset/callset.smk"

include: "99-outputs/align.smk"
include: "99-outputs/coverage.smk"
include: "99-outputs/calling.smk"

"""
Use this module to list all includes
required for your pipeline - do not
add your pipeline-specific modules
to "commons/00_commons.smk"
"""

include: "00-prepare/known_input.smk"
include: "00-prepare/sample_table.smk"
include: "00-prepare/settings.smk"

include: "10-align/pyutils.smk"
include: "10-align/hifi.smk"

include: "15-genotype/pangenie_prep.smk"
include: "15-genotype/pangenie_type.smk"
include: "15-genotype/merge_genotyped.smk"

include: "17-personal-ref/pyutils.smk"
include: "17-personal-ref/phasing.smk"

include: "20-postalign/process.smk"

include: "25-coverage/pyutils.smk"
include: "25-coverage/read_depth.smk"
include: "25-coverage/aggregate.smk"

include: "30-callshort/hifi.smk"

include: "40-callsv/hifi.smk"

include: "45-callcnv/pyutils.smk"
include: "45-callcnv/hifi.smk"

include: "50-postcall/10_split.smk"
include: "50-postcall/20_concat.smk"
include: "50-postcall/30_compress.smk"
include: "50-postcall/50_stats.smk"

include: "70-plotting/read_depth.smk"

include: "80-subset/alignments.smk"

include: "99-outputs/align.smk"
include: "99-outputs/coverage.smk"
include: "99-outputs/calling.smk"

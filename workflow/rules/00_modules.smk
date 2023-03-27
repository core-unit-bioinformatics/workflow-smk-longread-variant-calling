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
include: "20-postalign/merge.smk"

include: "30-callshort/hifi.smk"

include: "40-callsv/hifi.smk"

include: "99-outputs/align.smk"

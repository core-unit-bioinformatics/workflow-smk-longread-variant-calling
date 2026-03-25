###############################
### SETTINGS FOR VARIOUS TOOLS
###############################

# Affects alignment
# (reads are separated) and
# mosdepth read depth calculation
# ---
# Default is (or-chained):
# - read unmapped
# - read fails platform/vendor quality checks
# - read is PCR or optical duplicate
SAM_FLAG_DISCARD = config.get("sam_flag_discard", 1540)
assert isinstance(SAM_FLAG_DISCARD, int)

# Split final BAM files by default into
# main BAM containing primary/supplementary
# and and aux BAM containing secondary read
# alignments.
SAM_FLAG_SPLIT = config.get("sam_flag_split", 256)
assert isinstance(SAM_FLAG_SPLIT, int)

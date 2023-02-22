import enum

class ReadTypes(enum.Enum):
    """ TODO: stuff like this
    will potentially be unified
    in a separate (dev-?) tool
    repository to normalize sample
    sheets in an automated manner
    """
    hifi = 0
    ccs = 0
    pacbio_hifi = 0
    pacbio_ccs = 0
    hifi_reads = 0
    ccs_reads = 0
    ont = 1
    ont_reads = 1
    nano = 1
    nanopore = 1

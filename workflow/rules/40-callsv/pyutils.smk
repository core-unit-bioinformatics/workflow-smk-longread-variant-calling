
def load_min_mapq_threshold(wildcards):
    """TODO / future development
    Aligning the diploid readset against a
    diploid PRG results in a messed up
    MAPQ distribution with an largely
    inflated amount of lowQ / MAPQ 0 alignments
    in regions where both haplotypes of the
    PRG are identical / highly similar.
    There is currently no strategy to correct
    the MAPQ values for the diploid alignment
    case. Hence, the "workaround" to not throw
    away alignments based on their MAPQ just
    avoids throwing away variant alignments.
    It will, however, then keep alignments from
    regions such as centromeres that are
    canonically leading to MAPQ 0 alignments.
    """
    if wildcards.ref == "prg":
        return 0
    return MIN_MAPQ

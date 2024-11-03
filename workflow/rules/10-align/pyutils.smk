import itertools
import operator


def _group_wildcards(*args):
    """ TODO: candidate for inclusion
    in snakemake template.

    args: tuple of lists of tuples
    --- one list per wildcard (key)
    --- one (key, value) tuple per value for wildcard

    Groups wildcards by key to facilitate
    custom wildcard pairings where the usual
    zip does not work
    """
    groups = collections.defaultdict(set)

    for arg in args:
        for (key, value) in arg:
            groups[key].add(value)

    return groups


def expand_hifi_reads(*args):

    grouped_values = _group_wildcards(*args)

    assert all(s in HIFI_SAMPLES for s in grouped_values["sample"])
    assert all(p in HIFI_INPUT for p in grouped_values["path_id"])

    # NB: default combining all path_ids with all
    # samples does of course not work
    special_groups = ["sample", "path_id"]

    other_group_keys = tuple(
        k for k in grouped_values.keys() if k not in special_groups
    )

    get_other = operator.itemgetter(*other_group_keys)
    if len(other_group_keys) == 1:
        iter_other_comb = itertools.product(get_other(grouped_values))
    else:
        # NB: unpacking here
        iter_other_comb = itertools.product(*get_other(grouped_values))

    expand_wildcards = []
    for combination in iter_other_comb:
        for path in grouped_values["path_id"]:
            # TODO
            # ASM
            # This line assumes a 1-to-1 mapping between
            # sample and input files - for variant calling,
            # it rather seems that this is ok to work with
            sample = MAP_PATHID_TO_FILE_INFO[path]["sample"]
            assert isinstance(sample, str), f"Violated assumption: {sample}"
            wildcards = dict(
                (key, val) for key, val in zip(other_group_keys, combination)
            )
            wildcards["path_id"] = path
            wildcards["sample"] = sample
            expand_wildcards.append(wildcards)

    return expand_wildcards


def load_reference_genome(wildcards, index_file=False):

    if wildcards.ref in REF_GENOMES:
        if index_file:
            ref_file = REF_GENOMES[(wildcards.ref, "fai")]
        else:
            ref_file = REF_GENOMES[wildcards.ref]
    else:
        assert wildcards.ref == "prg"
        if hasattr(wildcards, "read_type"):
            read_type = wildcards.read_type
        else:
            read_type = "hifi"

        ####################################
        # DEBUG - TEMP DEV - DEVELOP - TODO
        panel_name = "hgsvc3hprc"
        prg_ref = "t2tv2"
        ####################################

        assert CONTROL_SAMPLES
        assert CASE_SAMPLES

        paired_sample = SAMPLE_PAIRS[wildcards.sample]

        if wildcards.sample in CASE_SAMPLES:
            assert paired_sample in CONTROL_SAMPLES
        else:
            # TODO - unclear decision ... align the
            # control sample reads to its own personalized
            # genome to check for shaky regions in the
            # alignment. However, given the intended use case
            # of low-cov controls, this will likely not be
            # very informative?
            paired_sample = wildcards.sample
            assert paired_sample in CONTROL_SAMPLES

        if index_file:
            ref_file = expand(
                rules.combine_consensus_haplotypes.output.fai,
                sample=paired_sample,
                read_type=read_type,
                ref=prg_ref,
                panel=panel_name
            )

        else:
            ref_file = expand(
                rules.combine_consensus_haplotypes.output.fasta,
                sample=paired_sample,
                read_type=read_type,
                ref=prg_ref,
                panel=panel_name
            )

    return ref_file


def load_reference_chromosomes(wildcards):
    """TODO [?] / IMPLICIT CONSTRAINT / HARDCODED ASSUMPTION
    this function encodes the assumption that a PRG
    is only constructed for the control
    samples (if configured by the user).
    Hence, a wildcards object containing
    a CASE sample swaps out that sample name
    with the associated CONTROL sample via
    the SAMPLE_PAIRS mapping.
    """

    if wildcards.ref in REF_GENOMES:
        chrom_list = CHROMOSOMES
    else:
        assert wildcards.ref == "prg"
        assert SAMPLE_PAIRS

        if wildcards.sample in CASE_SAMPLES:
            prg_sample = SAMPLE_PAIRS[wildcards.sample]
        else:
            prg_sample = wildcards.sample

        chrom_list = []
        for chrom in CHROMOSOMES:
            chrom_h1 = f"{chrom}.PRG.{prg_sample}.H1"
            chrom_h2 = f"{chrom}.PRG.{prg_sample}.H2"
            chrom_list.append(chrom_h1)
            chrom_list.append(chrom_h2)
        chrom_list = sorted(chrom_list)

    assert chrom_list

    return chrom_list

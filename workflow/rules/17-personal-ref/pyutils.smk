
def load_recombination_map(wildcards):

    try:
        map_for_chrom = config["recombination_maps"][wildcards.ref][wildcards.chrom]
    except KeyError:
        if wildcards.chrom == "chrY":
            # need to produce non-empty output for Snakemake
            map_for_chrom = config["recombination_maps"][wildcards.ref]["chrX"]
        else:
            raise

    file_path = DIR_GLOBAL_REF.joinpath(map_for_chrom)
    assert file_path.is_file()

    return file_path


def load_prg_variant_fasta(wildcards):

    assert hasattr(wildcards, "prg_variant")

    if wildcards.prg_variant == "prg":
        rules_file = rules.combine_consensus_haplotypes.output.fasta
    elif wildcards.prg_variant == "prg1":
        rules_file = expand(
            rules.generate_consensus_sequence.output.fasta,
            hap=1,
            allow_missing=True
        )
    elif wildcards.prg_variant == "prg2":
        rules_file = expand(
            rules.generate_consensus_sequence.output.fasta,
            hap=2,
            allow_missing=True
        )
    else:
        raise RuntimeError(f"Unknown PRG variant: {wildcards}")

    return rules_file

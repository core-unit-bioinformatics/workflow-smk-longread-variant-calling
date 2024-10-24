
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

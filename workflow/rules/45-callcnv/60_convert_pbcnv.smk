"""
(Potentially temporary?)
Dedicated module to convert the output
of HiFi-CNV (pbcnv) into a format that
is mock-compatible with CNVkit.
Intended use of that output is benefitting
from CNVkit's plotting capabilities downstream.
"""

rule add_unique_name_to_pbcnv_cn_track:
    """
    This rule: reading the output of "drop zero length"
    implies that all windows/bed regions should be valid
    for any downstream tools
    >> bigwigAverageOverBed requires a unique name field
    """
    input:
        bedgraph = rules.drop_zero_length_windows.output.bedgraph
    output:
        bed = temp(
            DIR_PROC.joinpath(
                "temp", "45-callcnv", "convert_pbcnv",
                "{sample}_hifi.{aligner}-pbcnv.{ref}.cn-uniq-name.bed.gz"
            )
        )
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt
    run:
        import pandas as pd
        import collections as col

        bed = pd.read_csv(input.bedgraph, sep="\t", header=None, names=["chrom", "start", "end", "cn"])

        record_counter = col.Counter()

        def derive_record_name(row, record_counter):
            record_counter[row.chrom] += 1
            chrom_record = record_counter[row.chrom]
            rec_name = f"{row.chrom}_SEG{chrom_record}_CN{row.cn}"
            return rec_name

        bed["name"] = bed.apply(derive_record_name, axis=1, args=(record_counter,))
        bed = bed[["chrom", "start", "end", "name", "cn"]]

        bed.to_csv(output.bed, sep="\t", header=False, index=False)
    # END RUN BLOCK


rule add_avg_minmax_read_depth_to_cn_track:
    input:
        bed = rules.add_unique_name_to_pbcnv_cn_track.output.bed,
        bigwig = rules.cnv_calling_pbcnv.output.depth
    output:
        tsv = temp(
            DIR_PROC.joinpath(
                "temp", "45-callcnv", "convert_pbcnv",
                "{sample}_hifi.{aligner}-pbcnv.{ref}.cn-read-depth.tsv.gz"
            )
        )
    conda:
        DIR_ENVS.joinpath("ucsctools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt
    shell:
        "bigWigAverageOverBed -bedOut=/dev/stdout -minMax {input.bigwig} {input.bed} /dev/null | gzip > {output.tsv}"


rule add_avg_minmax_log2_fc_to_cn_track:
    input:
        bed = rules.add_unique_name_to_pbcnv_cn_track.output.bed,
        bigwig = lambda wildcards: expand(
            rules.combine_depth_foldchange_track.output.foldchange,
            pairing=SAMPLE_TO_PAIRING[wildcards.sample],
            allow_missing=True
        )
    output:
        tsv = DIR_PROC.joinpath(
                "temp", "45-callcnv", "convert_pbcnv",
                "{sample}_hifi.{aligner}-pbcnv.{ref}.cn-log2-fc.tsv.gz"
            )
    conda:
        DIR_ENVS.joinpath("ucsctools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt
    shell:
        "bigWigAverageOverBed -bedOut=/dev/stdout -minMax {input.bigwig} {input.bed} /dev/null | gzip > {output.tsv}"


rule finalize_pbcnv_conversion_readdepth:
    """
    The output to .cns format follows the description
    from CNVkit's RTDs
    https://cnvkit.readthedocs.io/en/stable/fileformats.html
    subsection "Segmented log2 ratios (.cns)"
    """
    input:
        tsv = rules.add_avg_minmax_read_depth_to_cn_track.output.tsv
    output:
        tsv = DIR_RES.joinpath(
            "cnv_tracks", "pbcnv",
            "{sample}_hifi.{aligner}-pbcnv.{ref}.cn-read-depth.tsv.gz"
        ),
        cns = DIR_RES.joinpath(
            "cnv_tracks", "pbcnv", "cnvkit_convert",
            "{sample}_hifi.{aligner}-pbcnv.{ref}.rd.cns.gz"
        ),
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt
    run:
        import pandas as pd
        import numpy as np

        # this is hard-coded in pbcnv / hificnv at the moment ...
        PBCNV_BIN_SIZE = 2000

        table = pd.read_csv(
            input.tsv, sep="\t", header=None,
            names=["chrom", "start", "end", "name", "cn", "depth_avg", "depth_min", "depth_max"]
        )
        # direct dump: just add informative header ...
        table.to_csv(output.tsv, sep="\t", header=True, index=False)

        # drop unnecessary columns for CNS mock output
        table.drop(["depth_min", "depth_max"], axis=1, inplace=True)
        # add log2 column
        # add fudge factor to make log2 well-defined
        min_nz_depth = table.loc[table["depth_avg"] > 0, "depth_avg"].min()
        # any below?
        select_zeros = table["depth_avg"] < min_nz_depth
        if select_zeros.any():
            fudged_min = min_nz_depth / 10
            table.loc[select_zeros, "depth_avg"] = fudged_min
        table["depth_log2"] = np.log2(table["depth_avg"].values).round(3)
        assert not np.isinf(table["depth_log2"].values).any()
        table["length"] = table["end"] - table["start"]
        table["probes"] = (table["length"] / PBCNV_BIN_SIZE).abs().astype(int)

        # weight: manual (see above) does not specify exact details on how that is derived
        # and what value range is expected, so compute this in some reasonable manner
        # by assigning the weight relative to the average segment (region) size
        avg_size_by_chrom = table.groupby("chrom")["length"].mean()

        def compute_weight(row, avg_size):
            weight = (row.length / avg_size.at[row.chrom]).round(3)
            return weight

        table["weight"] = table.apply(compute_weight, axis=1, args=(avg_size_by_chrom,))
        table = table[["chrom", "start", "end", "name", "depth_log2", "depth_avg", "weight", "probes"]]
        table.rename(
            {
                "chrom": "chromosome",
                "name": "gene",
                "depth_log2": "log2",
                "depth_avg": "depth"

            }, axis=1, inplace=True
        )
        table.to_csv(output.cns, sep="\t", header=True, index=False)
    # END OF RUN BLOCK


rule finalize_pbcnv_conversion_foldchange:
    """
    The output to .cns format follows the description
    from CNVkit's RTDs
    https://cnvkit.readthedocs.io/en/stable/fileformats.html
    subsection "Segmented log2 ratios (.cns)"

    This rule can only be executed in a meaningful way if
    sample pairs are configured for this workflow
    """
    input:
        tsv = rules.add_avg_minmax_log2_fc_to_cn_track.output.tsv,
        depth = rules.finalize_pbcnv_conversion_readdepth.output.tsv
    output:
        tsv = DIR_RES.joinpath(
            "cnv_tracks", "pbcnv",
            "{sample}_hifi.{aligner}-pbcnv.{ref}.cn-log2-fc.tsv.gz"
        ),
        cns = DIR_RES.joinpath(
            "cnv_tracks", "pbcnv", "cnvkit_convert",
            "{sample}_hifi.{aligner}-pbcnv.{ref}.fc.cns.gz"
        ),
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt
    run:
        import pandas as pd
        import numpy as np

        # this is hard-coded in pbcnv / hificnv at the moment ...
        PBCNV_BIN_SIZE = 2000

        table = pd.read_csv(
            input.tsv, sep="\t", header=None,
            names=["chrom", "start", "end", "name", "cn", "log2fc_avg", "log2fc_min", "log2fc_max"]
        )
        # direct dump: just add informative header ...
        table.to_csv(output.tsv, sep="\t", header=True, index=False)

        # drop unnecessary columns for CNS mock output
        table.drop(["log2fc_min", "log2fc_max"], axis=1, inplace=True)
        assert not np.isinf(table["log2fc_avg"].values).any()
        table["length"] = table["end"] - table["start"]
        table["probes"] = (table["length"] / PBCNV_BIN_SIZE).abs().astype(int)

        # weight: manual (see above) does not specify exact details on how that is derived
        # and what value range is expected, so compute this in some reasonable manner
        # by assigning the weight relative to the average segment (region) size
        avg_size_by_chrom = table.groupby("chrom")["length"].mean()

        def compute_weight(row, avg_size):
            weight = (row.length / avg_size.at[row.chrom]).round(3)
            return weight

        # pull in read depth information from other table
        depth_table = pd.read_csv(input.depth, sep="\t", header=0)

        table.insert(5, "depth_avg", depth_table["depth_avg"])
        table["weight"] = table.apply(compute_weight, axis=1, args=(avg_size_by_chrom,))
        table = table[["chrom", "start", "end", "name", "log2fc_avg", "depth_avg", "weight", "probes"]]
        table.rename(
            {
                "chrom": "chromosome",
                "name": "gene",
                "depth_log2": "log2",
                "depth_avg": "depth"

            }, axis=1, inplace=True
        )
        table.to_csv(output.cns, sep="\t", header=True, index=False)
    # END OF RUN BLOCK


# manually assemble the expected output of this module
# due to the conditional sample group structure
_CONVERT_PBCNV_OUTPUT = []

if SAMPLE_PAIRS is not None and RUN_HIFI_CNV_CALLING_TOOLCHAIN:
    _CONVERT_PBCNV_OUTPUT.extend(
        expand(
            rules.finalize_pbcnv_conversion_foldchange.output.cns,
            sample=PAIRED_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbcnv", "hifi")],
            ref=USE_REF_GENOMES
        )
    )

if RUN_HIFI_CNV_CALLING_TOOLCHAIN:
    _CONVERT_PBCNV_OUTPUT.extend(
        expand(
            rules.finalize_pbcnv_conversion_readdepth.output.cns,
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbcnv", "hifi")],
            ref=USE_REF_GENOMES,
        )
    )

rule run_all_pbcnv_cnvkit_conversion:
    input:
        module_files = _CONVERT_PBCNV_OUTPUT

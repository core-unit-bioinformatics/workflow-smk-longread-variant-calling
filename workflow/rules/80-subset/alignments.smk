

localrules: add_margin_around_roi
rule add_margin_around_roi:
    input:
        bed = lambda wildcards: USER_ROI_FILES[wildcards.roi],
        fai = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")]
    output:
        ext_bed = DIR_LOCAL_REF.joinpath(
            "{ref}.{roi}.ext-{margin}.bed"
        )
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt
    run:
        import pandas as pd
        chrom_sizes = dict(
            (row.chrom, row.size) for row in (
                pd.read_csv(
                    input.fai, sep="\t", header=None, names=["chrom", "size"], usecols=[0,1]
                )
            ).itertuples()
        )

        try:
            margin_bp = int(wildcards.margin)
        except ValueError:
            assert wildcards.margin.endswith("k")
            margin_bp = int(int(wildcards.margin[:-1]) * 1000)

        regions = pd.read_csv(input.bed, sep="\t", header=None, name=["chrom", "start", "end", "name"], usecols=[0,1,2,3])

        def adapt_clip_end(row, margin, chrom_sizes):
            return min(row.end + margin, chrom_sizes[row.chrom])


        if margin_bp > 0:
            regions["start"] = regions["start"] - margin_bp
            regions["start"].clip(lower=0, inplace=True)

            regions["end"] = regions.apply(adapt_clip_end, axis=1, args=(margin_bp, chrom_sizes))

        regions.to_csv(output.ext_bed, sep="\t", header=False, index=False)
    # END OF RUN BLOCK


rule extract_alignment_subset:
    input:
        bam = rules.split_merged_alignments.output.main,
        bai = rules.split_merged_alignments.output.main_bai,
        bed = rules.add_margin_around_roi.output.ext_bed
    output:
        bam = DIR_RES.joinpath(
            "alignments", "roi_subsets", "{ref}.{roi}",
            "{sample}_{read_type}.{aligner}.{ref}.{roi}.ext-{margin}.main.sort.bam"
        ),
        bai = DIR_RES.joinpath(
            "alignments", "roi_subsets", "{ref}.{roi}",
            "{sample}_{read_type}.{aligner}.{ref}.{roi}.ext-{margin}.main.sort.bam.bai"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "samtools view --bam --threads {threads} --region-file {input.bed} --output {output.bam} {input.bam}"
            " && "
        "samtools index {output.bam}"


if HIFI_SAMPLES:

    rule run_all_extract_roi_hifi_alignment_subset:
        input:
            bam = expand(
                DIR_RES.joinpath(
                    "alignments", "roi_subsets", "{ref_roi}",
                    "{sample}_{read_type}.{aligner}.{ref_roi}.ext-{margin}.main.sort.bam"
                ),
                sample=HIFI_SAMPLES,
                read_type=["hifi"],
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref_roi=USER_ROI_FILE_WILDCARDS,
                margin=config.get("roi_region_margin", "10k")

            ),
            bai = expand(
                DIR_RES.joinpath(
                    "alignments", "roi_subsets", "{ref_roi}",
                    "{sample}_{read_type}.{aligner}.{ref_roi}.ext-{margin}.main.sort.bai"
                ),
                sample=HIFI_SAMPLES,
                read_type=["hifi"],
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref_roi=USER_ROI_FILE_WILDCARDS,
                margin=config.get("roi_region_margin", "10k")

            ),

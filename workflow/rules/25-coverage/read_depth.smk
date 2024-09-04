
rule compute_read_depth_in_windows:
    input:
        bam = rules.split_merged_alignments.output.main,
        bai = rules.split_merged_alignments.output.main_bai
    output:
        check = DIR_PROC.joinpath(
            "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}",
            "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}.ok",
        ),  # check output - convenience for prefix generation; see below
        collect = multiext(
            str(DIR_PROC.joinpath(
                "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}",
                "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}",
            )),
            ".mosdepth.global.dist.txt", ".mosdepth.region.dist.txt", ".mosdepth.summary.txt",
            ".quantized.bed.gz", ".quantized.bed.gz.csi",
            ".regions.bed.gz", ".regions.bed.gz.csi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    params:
        prefix=lambda wildcards, output: pathlib.Path(output.check).with_suffix(""),
        min_mapq=lambda wildcards: int(wildcards.mapq),
        quantize_steps=lambda wildcards: ":".join(map(str, MOSDEPTH_QUANTIZE_STEPS)) + ":",
        quantize_names=MOSDEPTH_QUANTIZE_NAMES,  # does nothing, just memory hook
        export_quant_names=assemble_mosdepth_quantize_export(
            MOSDEPTH_QUANTIZE_STEPS, MOSDEPTH_QUANTIZE_NAMES
        ),
        window_size=MOSDEPTH_WINDOW_SIZE,
        wd=lambda wildcards, output: pathlib.Path(output.check).parent
    shell:
        "mkdir -p {params.wd}"
            " && "
        "{params.export_quant_names}"
        "mosdepth --use-median --mapq {params.min_mapq} --threads {threads} "
        "--by {params.window_size} --no-per-base "
        "--quantize {params.quantize_steps} {params.prefix} {input.bam}"
            " && "
        "touch {output.check}"


rule compute_read_depth_in_user_roi:
    input:
        user_roi = lambda wildcards: USER_ROI_FILES[wildcards.roi],
        bam = rules.split_merged_alignments.output.main,
        bai = rules.split_merged_alignments.output.main_bai
    output:
        check = DIR_PROC.joinpath(
            "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}",
            "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}.ok",
        ),
        collect = multiext(
            str(DIR_PROC.joinpath(
                "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}",
                "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}",
            )),
            ".mosdepth.global.dist.txt", ".mosdepth.region.dist.txt", ".mosdepth.summary.txt",
            ".thresholds.bed.gz", ".thresholds.bed.gz.csi",
            ".regions.bed.gz", ".regions.bed.gz.csi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 4096 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    params:
        prefix=lambda wildcards, output: pathlib.Path(output.check).with_suffix(""),
        min_mapq=lambda wildcards: int(wildcards.mapq),
        thresholds=lambda wildcards: ",".join(map(str, MOSDEPTH_COV_THRESHOLDS)),
        wd=lambda wildcards, output: pathlib.Path(output.check).parent
    shell:
        "mkdir -p {params.wd}"
            " && "
        "mosdepth --use-median --mapq {params.min_mapq} --threads {threads} "
        "--by {input.user_roi} --no-per-base "
        "--thresholds {params.thresholds} {params.prefix} {input.bam}"
            " && "
        "touch {output.check}"


# TODO this does not strike me as very elegant
if HIFI_SAMPLES:

    _HIFI_DEPTH_ROI_REGIONS = expand(
        DIR_PROC.joinpath(
            "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}",
            "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}.regions.bed.gz"
        ),
        sample=HIFI_SAMPLES,
        mapq=MOSDEPTH_MIN_MAPQ,
        allow_missing=True
    )

if ONT_SAMPLES:

    _ONT_DEPTH_ROI_REGIONS = expand(
        DIR_PROC.joinpath(
            "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}",
            "{sample}_{read_type}.{aligner}.{ref}.{roi}.mq{mapq}.regions.bed.gz"
        ),
        sample=ONT_SAMPLES,
        mapq=MOSDEPTH_MIN_MAPQ,
        allow_missing=True
    )


rule merge_read_depth_in_user_roi:
    input:
        regions = lambda wildcards: _HIFI_DEPTH_ROI_REGIONS if wildcards.read_type == "hifi" else _ONT_DEPTH_ROI_REGIONS
    output:
        merged = DIR_RES.joinpath(
            "read_depth", "user_roi",
            "SAMPLES.{read_type}.{aligner}.{ref}.{roi}.regions.tsv.gz"
        )
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt
    run:
        import pandas as pd
        import pathlib as pl

        splitter = f"_{wildcards.read_type}"
        concat = []
        for bed_file in sorted(input.regions):
            bed_file_name = pl.Path(bed_file).name
            sample = bed_file_name.split(splitter)[0]
            assert sample in HIFI_SAMPLES or sample in ONT_SAMPLES
            mapq = bed_file_name.split(".")[-4]
            assert mapq.startswith("mq")

            cov_column = f"{sample}_{mapq}_cov"
            cov_data = pd.read_csv(bed_file, sep="\t", header=["chrom", "start", "end", "name", cov_column])
            cov_data.set_index(["chrom", "start", "end", "name"], inplace=True)
            concat.append(cov_data)

        concat = pd.concat(concat, axis=1, ignore_index=False)
        concat.to_csv(output.merged, sep="\t", header=True, index=True)
    # END OF RUN BLOCK


if HIFI_SAMPLES:
    rule compute_genome_hifi_read_depth:
        input:
            md_ok = expand(
                DIR_PROC.joinpath(
                    "25-coverage", "{sample}_hifi.{aligner}.{ref}.win.mq{mapq}",
                    "{sample}_hifi.{aligner}.{ref}.win.mq{mapq}.ok",
                ),
                sample=HIFI_SAMPLES,
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ
            )

    if USER_ROI_FILE_WILDCARDS:
        rule compute_roi_hifi_read_depth:
            input:
                md_ok = expand(
                    DIR_PROC.joinpath(
                        "25-coverage", "{sample}_hifi.{aligner}.{ref_roi_pair}.mq{mapq}",
                        "{sample}_hifi.{aligner}.{ref_roi_pair}.mq{mapq}.ok",
                    ),
                    sample=HIFI_SAMPLES,
                    aligner=HIFI_ALIGNER_WILDCARDS,
                    ref_roi_pair=USER_ROI_FILE_WILDCARDS,
                    mapq=MOSDEPTH_MIN_MAPQ
                ),
                merged = expand(
                    DIR_RES.joinpath(
                        "read_depth", "user_roi", "SAMPLES.{read_type}.{aligner}.{ref_roi_pair}.regions.tsv.gz"
                    ),
                    read_type="hifi",
                    aligner=HIFI_ALIGNER_WILDCARDS,
                    ref_roi_pair=USER_ROI_FILE_WILDCARDS
                )


if ONT_SAMPLES:
    rule compute_genome_ont_read_depth:
        input:
            md_ok = expand(
                DIR_PROC.joinpath(
                    "25-coverage", "{sample}_ont.{aligner}.{ref}.win.mq{mapq}",
                    "{sample}_ont.{aligner}.{ref}.win.mq{mapq}.ok",
                ),
                sample=ONT_SAMPLES,
                aligner=ONT_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ
            )

    if USER_ROI_FILE_WILDCARDS:
        rule compute_roi_ont_read_depth:
            input:
                md_ok = expand(
                    DIR_PROC.joinpath(
                        "25-coverage", "{sample}_ont.{aligner}.{ref_roi_pair}.mq{mapq}",
                        "{sample}_ont.{aligner}.{ref_roi_pair}.mq{mapq}.ok",
                    ),
                    sample=ONT_SAMPLES,
                    aligner=ONT_ALIGNER_WILDCARDS,
                    ref_roi_pair=USER_ROI_FILE_WILDCARDS,
                    mapq=MOSDEPTH_MIN_MAPQ
                ),
                merged = expand(
                    DIR_RES.joinpath(
                        "read_depth", "user_roi", "SAMPLES.{read_type}.{aligner}.{ref_roi_pair}.regions.tsv.gz"
                    ),
                    read_type="ont",
                    aligner=ONT_ALIGNER_WILDCARDS,
                    ref_roi_pair=USER_ROI_FILE_WILDCARDS
                )

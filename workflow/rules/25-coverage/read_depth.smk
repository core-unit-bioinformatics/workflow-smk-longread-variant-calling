
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
                )

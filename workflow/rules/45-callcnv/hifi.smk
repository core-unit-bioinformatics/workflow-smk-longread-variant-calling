
rule cnv_calling_pbcnv:
    """HIFICNV sets the output file names
    by using an output prefix plus the sample
    name included in the input BAM file.
    """
    input:
        ref = lambda wildcards: REF_GENOMES[wildcards.ref],
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")],
        cn_noise = lambda wildcards: load_cn_aux_file(wildcards.ref, wildcards.sample, "noise"),
        cn_expect = lambda wildcards: load_cn_aux_file(wildcards.ref, wildcards.sample, "expect"),
        bam = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam"
        ),
        bai = DIR_PROC.joinpath(
            "20-postalign", "{sample}_hifi.{aligner}.{ref}.sort.bam.bai"
        ),
    output:
        check = DIR_PROC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}",
            "hificnv.run.ok"
        )
    log:
        DIR_LOG.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}.log"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}.rsrc"
        )
    conda:
        DIR_ENVS.joinpath("caller", "pbcnv.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb = lambda wildcards, attempt: 24576 * attempt,
        time_hrs = lambda wildcards, attempt: attempt
    params:
        outprefix = lambda wildcards, output: str(pathlib.Path(output.check).parent) + "/"
    shell:
        "hificnv --ref {input.ref} --bam {input.bam} --exclude {input.cn_noise} "
        "--expected-cn {input.cn_expect} --threads {threads} "
        "--output-prefix {params.outprefix} &> {log}"
            " && "
        "touch {output.check}"


rule run_all_cnv_calling_pbcnv:
    input:
        ok = expand(
            rules.cnv_calling_pbcnv.output.check,
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbcnv", "hifi")],
            ref=["hg38"]
        )  # DEBUG for now: only aux files source is hg38

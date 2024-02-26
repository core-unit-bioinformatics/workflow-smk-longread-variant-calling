
rule cnv_calling_pbcnv:
    """HIFICNV sets the output file names
    by using an output prefix plus the sample
    name included in the input BAM file.
    Hence, the prefix below includes the "out" to
    make pbcnv/hificnv recognize the path as a folder.
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
        copynum = DIR_PROC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}",
            "out.{sample}.copynum.bedgraph"
        ),
        depth = DIR_PROC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}",
            "out.{sample}.depth.bw"
        ),
        vcf = DIR_PROC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}",
            "out.{sample}.vcf.gz"
        ),
    log:
        DIR_PROC.joinpath(
            "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}",
            "out.log"
        ),
    # NB: the log output path is hard-coded in hificnv/pbcnv;
    # it is what it is (and that is the above...)
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
        outprefix = lambda wildcards, output: pathlib.Path(output.vcf).parent.joinpath("out")
    shell:
        "hificnv --ref {input.ref} --bam {input.bam} --exclude {input.cn_noise} "
        "--expected-cn {input.cn_expect} --threads {threads} "
        "--output-prefix {params.outprefix}"


rule run_all_cnv_calling_pbcnv:
    input:
        cn_est = expand(
            rules.cnv_calling_pbcnv.output.copynum,
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbcnv", "hifi")],
            ref=USE_REF_GENOMES
        )

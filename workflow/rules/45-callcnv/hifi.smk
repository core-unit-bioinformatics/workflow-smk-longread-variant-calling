
localrules: create_reference_windows
rule create_reference_windows:
    """The reference windows are just used
    to simplify the merging process of the
    hificnv bedgraph output
    """
    input:
        ref_idx = lambda wildcards: REF_GENOMES[(wildcards.ref, "fai")]
    output:
        bed = DIR_LOCAL_REF.joinpath(
            "{ref}.windows.{win_size}.bed.gz"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    params:
        win_size = lambda wildcards: suffixed_number_to_int(wildcards.win_size)
    shell:
        "bedtools makewindows -g {input.ref_idx} -w {params.win_size}"
            " | "
        "gzip > {output}"


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
        bam = expand(
            rules.split_merged_alignments.output.main,
            read_type="hifi",
            allow_missing=True
        ),
        bai = expand(
            rules.split_merged_alignments.output.main_bai,
            read_type="hifi",
            allow_missing=True
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


rule intersect_copynum_windows:
    """This rule simplifies/normalizes the copy number
    track to regular windows to make merging with/comparing to
    other samples trivial.
    The operations below just cut out the copy number estimate
    and replace the empty '.' with -1
    (happens in masked regions such as centromeres)
    """
    input:
        windows = rules.create_reference_windows.output.bed,
        cn_track = rules.cnv_calling_pbcnv.output.copynum
    output:
        bed = DIR_PROC.joinpath(
            "45-callcnv", "normalized",
            "{sample}_hifi.{aligner}-pbcnv.{ref}.win-{win_size}.cn.bed.gz",
        ),
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt
    shell:
        "bedtools intersect -wao -a {input.windows} -b {input.cn_track}"
            " | "
        "cut -f 1,2,3,7"
            " | "
        "sed 's/\./-1/g'"
            " | "
        "gzip > {output}"


rule run_all_cnv_calling_pbcnv:
    input:
        cn_est = expand(
            rules.intersect_copynum_windows.output.bed,
            sample=HIFI_SAMPLES,
            aligner=ALIGNER_FOR_CALLER[("pbcnv", "hifi")],
            ref=USE_REF_GENOMES,
            win_size=["1k"]
        )

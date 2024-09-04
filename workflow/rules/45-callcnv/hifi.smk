
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


rule drop_zero_length_windows:
    """hificnv has the somewhat problematic property of producing
    zero-length BED regions that break the subsequent call to
    bedtools intersect. See
    gh#29 // github.com/PacificBiosciences/HiFiCNV
    gh#869 // github.com/arq5x/bedtools2
    Hence, these records are removed here but if and only if
    both coordinates are zero (start and end)
    """
    input:
        bedgraph = rules.cnv_calling_pbcnv.output.copynum
    output:
        bedgraph = temp(
            DIR_PROC.joinpath("temp", "45-callcnv", "{sample}_hifi.{aligner}-pbcnv.{ref}.cn.bedgraph")
        )
    run:
        import pandas as pd
        cn_track = pd.read_csv(input.bedgraph, sep="\t", header=None, names=["chrom", "start", "end", "cn"])
        select_zero_start = cn_track["start"] == 0
        select_zero_end = cn_track["end"] == 0
        selector = select_zero_start & select_zero_end
        if selector.any():
            sub = cn_track.loc[~selector, :]
        else:
            sub = cn_track
        sub.to_csv(output.bedgraph, sep="\t", header=False, index=False)
    # END OF RUN BLOCK


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
        cn_track = rules.drop_zero_length_windows.output.bedgraph
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

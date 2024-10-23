
rule merge_decompress_reads:
    """
    NB / TODO
    PanGenie does not eat gzip-compressed files,
    so the input here immediately accesses the ".stem"
    attribute of the assumed pathlib.Path object.
    Likely to break ...

    Note that a wd-local copy of the reads also
    avoids introducing path bindings for
    the singularity/apptainer version of PanGenie
    """
    input:
        reads=lambda wildcards: MAP_SAMPLE_TO_INPUT_FILES[wildcards.sample][wildcards.read_type]["paths"]
    output:
        reads=temp(
            DIR_PROC.joinpath("15-genotype", "prep_reads", "{sample}_{read_type}.merged.fastq")
        )
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: 1 * attempt
    shell:
        "gzip -c -d {input.reads} > {output.reads}"


rule run_pangenie_genotyping:
    """This rule uses a pre-built panel
    for testing / development purposes and
    thus hardcodes the '-a 108' parameter
    that is a function of the input panel vcf

    NB / TODO
    PanGenie does not eat gzip-compressed files,
    so the input here immediately accesses the ".stem"
    attribute of the assumed pathlib.Path object.
    Likely to break ...

    """
    input:
        pgi = rules.build_pangenie_index.output.pgi,
        reads = rules.merge_decompress_reads.output.reads
    output:
        vcf = DIR_PROC.joinpath(
            "15-genotype", "pangenie_genotyping", "{sample}.{ref}.{panel}",
            "{sample}_{read_type}_{ref}_{panel}_genotyping.vcf"
        )
    log:
        DIR_LOG.joinpath(
            "15-genotype", "pangenie_genotyping",
            "{sample}_{read_type}_{ref}_{panel}.pgtype.log"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "15-genotype", "pangenie_index",
            "{sample}_{read_type}_{ref}_{panel}.pgtype.rsrc"
        )
    singularity:
        str(CONTAINER_STORE.joinpath(config.get("pangenie_container", "no-pangenie-container")))
    threads: CPU_HIGH
    resources:
        mem_mb=lambda wildcards, attempt: 81920 + 16384 * attempt,
        time_hrs=lambda wildcards, attempt: 5 * attempt
    params:
        pgi_prefix=lambda wildcards, input: pathlib.Path(input.pgi).joinpath("idx"),
        out_prefix=lambda wildcards, output: str(output.vcf).rsplit("_", 1)[0]
    shell:
        "PanGenie -t {threads} -j {threads} -a 108 -s {wildcards.sample} "
        "-f {params.pgi_prefix} -o {params.out_prefix} "
        "-i {input.reads} &> {log}"


rule run_all_pangenie_genotyping:
    input:
        vcfs = expand(
            rules.run_pangenie_genotyping.output.vcf,
            sample=SAMPLES,
            read_type=["hifi"],
            ref=["t2tv2"],
            panel=["hgsvc3hprc"]
        )

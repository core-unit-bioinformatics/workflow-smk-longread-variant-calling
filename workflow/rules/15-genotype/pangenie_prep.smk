
rule decompress_reference_file:
    """This just exists for PanGenie
    """
    input:
        ref_file = DIR_GLOBAL_REF.joinpath("{filename}.gz")
    output:
        ref_file = DIR_LOCAL_REF.joinpath("{filename}")
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt,
        time_hrs=lambda wildcards, attempt: 1 * attempt
    shell:
        "gzip -d -c {input} > {output}"


rule build_pangenie_index:
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
        vcf = lambda wildcards: DIR_LOCAL_REF.joinpath(
            config["panel_vcfs"][wildcards.panel]["multiallelic"]
        ).with_suffix(""),
        ref_genome = lambda wildcards: REF_GENOMES[wildcards.ref]
    output:
        pgi = directory(
            DIR_PROC.joinpath("15-genotype", "pangenie_index", "{ref}.{panel}.pgi")
        )
    log:
        DIR_LOG.joinpath(
            "15-genotype", "pangenie_index",
            "{ref}.{panel}.pgidx.log"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "15-genotype", "pangenie_index",
            "{ref}.{panel}.pgidx.rsrc"
        )
    singularity:
        str(CONTAINER_STORE.joinpath(config.get("pangenie_container", "no-pangenie-container")))
    threads: CPU_HIGH
    resources:
        mem_mb=lambda wildcards, attempt: 81920 + 16384 * attempt,
        time_hrs=lambda wildcards, attempt: 5 * attempt
    params:
        pgi_prefix=lambda wildcards, output: pathlib.Path(output.pgi).joinpath("idx")
    shell:
        "mkdir -p {output.pgi}"
            " && "
        "PanGenie-index -t {threads} -v {input.vcf} -r {input.ref_genome} -o {params.pgi_prefix} &> {log}"

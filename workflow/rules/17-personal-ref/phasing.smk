

rule dump_list_of_males:
    output:
        lst = DIR_PROC.joinpath(
            "17-personal-ref", "male_samples.list"
        )
    run:
        male_samples = []
        for sample, sex in SAMPLE_SEX.items():
            if sex != "male":
                continue
            # we are in the code path for the PRGs,
            # listing should only contain relevant
            # male samples as specified via paired samples
            if sample in PAIRED_CASES or sample in PAIRED_CONTROLS:
                male_samples.append(sample)

        with open(output.lst, "w") as dump:
            _ = dump.write("\n".join(sorted(male_samples)) + "\n")
    # END OF RUN BLOCK


rule fix_ref_panel_vcf:
    """The ref panel vcf (decomposed)
    does not contain all tags and AC
    is required for SHAPEIT to work.
    This rule is somewhat data-specific
    and may be obsolete in the future.

    The Python script just hardcodes
    QUAL and FILTER to 60 and PASS for
    all calls in the ref panel. Unclear
    if SHAPEIT looks at those values at all,
    just a precaution.
    """
    input:
        ref_panel = lambda wildcards: DIR_GLOBAL_REF.joinpath(
            config["panel_vcfs"][wildcards.panel]["biallelic"]
        )
    output:
        ref_panel = DIR_LOCAL_REF.joinpath(
            "{ref}_{panel}.balc.vcf.gz"
        ),
        tbi = DIR_LOCAL_REF.joinpath(
            "{ref}_{panel}.balc.vcf.gz.tbi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_MEDIUM
    resources:
        mem_mb=lambda wildcards, attempt: 16384 * attempt,
        time_hrs=lambda wildcards, attempt: attempt * attempt
    params:
        script=find_script("set_qual_filter")
    shell:
        "bcftools view --output-type v {input.ref_panel}"
            " | "
        "{params.script}"
            " | "
        "bcftools plugin fill-tags --threads {threads} --output-type z9 --output {output.ref_panel} /dev/stdin -- -t AN,AC,AF"
            " && "
        "tabix -p vcf --threads {threads} {output.ref_panel}"


rule phase_samples_by_chrom:
    input:
        vcf = lambda wildcards: expand(
            rules.region_merge_and_fill_sample_genotypes.output.vcf,
            chrom=wildcards.chrom,
            allele_repr="balc",
            allow_missing=True
        ),
        tbi = lambda wildcards: expand(
            rules.region_merge_and_fill_sample_genotypes.output.tbi,
            chrom=wildcards.chrom,
            allele_repr="balc",
            allow_missing=True
        ),
        male_samples = rules.dump_list_of_males.output.lst,
        recomb_map = load_recombination_map,
        ref_panel = rules.fix_ref_panel_vcf.output.ref_panel
    output:
        bcf = DIR_PROC.joinpath(
            "17-personal-ref", "phasing_by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.{chrom}.bcf"
        )
    log:
        DIR_LOG.joinpath(
            "17-personal-ref", "phasing_by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.{chrom}.shapeit.log"
        )
    benchmark:
        DIR_RSRC.joinpath(
            "17-personal-ref", "phasing_by_chrom",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.{chrom}.shapeit.rsrc"
        )
    conda:
        DIR_ENVS.joinpath("phasing.yaml")
    threads: CPU_HIGH
    resources:
        mem_mb=lambda wildcards, attempt: 16384 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    params:
        haploids=lambda wildcards, input: f"--haploids {input.male_samples}" if wildcards.chrom in ["chrX", "chrY"] else "",
        recmap=lambda wildcards, input: f"--map {input.recomb_map}" if wildcards.chrom != "chrY" else ""
    shell:
        "SHAPEIT5_phase_common --input {input.vcf} --reference {input.ref_panel} --region {wildcards.chrom} {params.haploids} {params.recmap} --output {output.bcf} --thread {threads} &> {log}"


# DEBUG / TODO
# SHAPEIT simply segfaults w/o recombination map file,
# which obviously does not exist for chrY. Really annoying
# that tools are not designed to process a complete human genome ...
# Unclear: "the phasing" for chrY has to be implemented in some
# post-processing step
_TEMP_FIX_CHROMOSOMES = [c for c in CHROMOSOMES if c != "chrY"]
_TEMP_CONSTRAINT_CHROMOSOMES = "(" + "|".join(_TEMP_FIX_CHROMOSOMES) + ")"

rule convert_phased_to_vcf:
    input:
        bcf = rules.phase_samples_by_chrom.output.bcf
    output:
        vcf = DIR_PROC.joinpath(
            "17-personal-ref", "convert_vcf",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.{chrom}.vcf.gz"
        ),
        tbi = DIR_PROC.joinpath(
            "17-personal-ref", "convert_vcf",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.{chrom}.vcf.gz.tbi"
        )
    wildcard_constraints:
        chrom=_TEMP_CONSTRAINT_CHROMOSOMES
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "bcftools view --threads {threads} --output-type z9 --output {output.vcf} {input.bcf}"
            " && "
        "tabix -p vcf --threads {threads} {output.vcf}"


rule mock_phase_males:
    input:
        vcf = lambda wildcards: expand(
            rules.region_merge_and_fill_sample_genotypes.output.vcf,
            chrom="chrY",
            allele_repr="balc",
            allow_missing=True
        ),
        tbi = lambda wildcards: expand(
            rules.region_merge_and_fill_sample_genotypes.output.tbi,
            chrom="chrY",
            allele_repr="balc",
            allow_missing=True
        ),
        male_samples = rules.dump_list_of_males.output.lst,
    output:
        vcf = DIR_PROC.joinpath(
            "17-personal-ref", "convert_vcf",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.chrY.vcf.gz"
        ),
        tbi = DIR_PROC.joinpath(
            "17-personal-ref", "convert_vcf",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.chrY.vcf.gz.tbi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 4096 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    params:
        script=find_script("mock_phase_males")
    shell:
        "bcftools view --output-type v {input.vcf}"
            " | "
        "{params.script} --male-samples {input.male_samples}"
            " | "
        "bcftools view --output-type z9 --threads {threads} --output {output.vcf}"
            " && "
        "tabix -p vcf --threads {threads} {output.vcf}"



localrules: create_phased_vcf_fofn
rule create_phased_vcf_fofn:
    input:
        vcfs = expand(
            rules.convert_phased_to_vcf.output.vcf,
            chrom=_TEMP_FIX_CHROMOSOMES,
            allow_missing=True
        ),
        tbi = expand(
            rules.convert_phased_to_vcf.output.tbi,
            chrom=_TEMP_FIX_CHROMOSOMES,
            allow_missing=True
        ),
        vcf_chry = rules.mock_phase_males.output.vcf,
        vcf_tbi = rules.mock_phase_males.output.tbi
    output:
        lst = DIR_PROC.joinpath(
            "17-personal-ref", "convert_vcf",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.wg.lst"
        ),
    run:
        import pathlib as pl
        import io as io

        buffer = io.StringIO()
        for rel_path in sorted(input.vcfs):
            full_path = WORKDIR.joinpath(rel_path)
            assert full_path.is_file()
            buffer.write(rel_path + "\n")

        full_path = WORKDIR.joinpath(input.vcf_chry)
        assert full_path.is_file()
        buffer.write(input.vcf_chry + "\n")

        with open(output.lst, "w") as dump:
            _ = dump.write(buffer.getvalue())
    # END OF RUN BLOCK


rule concat_phased_chrom_vcfs:
    input:
        lst = rules.create_phased_vcf_fofn.output.lst
    output:
        vcf = DIR_RES.joinpath(
            "genotyping", "phased",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.wg.vcf.gz"
        ),
        tbi = DIR_RES.joinpath(
            "genotyping", "phased",
            "SAMPLES_{read_type}_{ref}_{panel}.ps.wg.vcf.gz.tbi"
        )
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "bcftools concat --threads {threads} --file-list {input.lst} --output-type z9 --output {output.vcf}"
            " && "
        "tabix -p vcf --threads {threads} {output.vcf}"


rule run_all_phase_genotyped_samples:
    input:
        vcf = expand(
            rules.concat_phased_chrom_vcfs.output.vcf,
            read_type=["hifi"],
            ref=["t2tv2"],
            panel=["hgsvc3hprc"]
        ),
        tbi = expand(
            rules.concat_phased_chrom_vcfs.output.tbi,
            read_type=["hifi"],
            ref=["t2tv2"],
            panel=["hgsvc3hprc"]
        ),

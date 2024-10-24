

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
            male_samples.append(sample)

        with open(output.lst, "w") as dump:
            _ = dump.write("\n".join(sorted(male_samples)) + "\n")
    # END OF RUN BLOCK


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
        recomb_map = load_recombination_map
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
        mem_mb=lambda wildcards, attempt: 32768 + 32768 * attempt,
        time_hrs=lambda wildcards, attempt: 11 * attempt
    params:
        haploids=lambda wildcards, input: f"--haploids {input.male_samples}" if wildcards.chrom in ["chrX", "chrY"] else "",
        recmap=lambda wildcards, input: f"--map {input.recomb_map}" if wildcards.chrom != "chrY" else ""
    shell:
        "SHAPEIT5_phase_common --input {input.vcf} --region {wildcards.chrom} {params.haploids} {params.recmap} --output {output.bcf} --thread {threads} &> {log}"


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
    conda:
        DIR_ENVS.joinpath("biotools.yaml")
    threads: CPU_LOW
    resources:
        mem_mb=lambda wildcards, attempt: 2048 * attempt,
        time_hrs=lambda wildcards, attempt: attempt
    shell:
        "bcftools view --threads {threads} --output-format z9 --output {output.vcf} {input.bcf}"
            " && "
        "tabix -p vcf --threads {threads} {output.vcf}"


localrules: create_phased_vcf_fofn
rule create_phased_vcf_fofn:
    input:
        vcfs = expand(
            rules.convert_phased_to_vcf.output.vcf,
            chrom=CHROMOSOMES,
            allow_missing=True
        ),
        tbi = expand(
            rules.convert_phased_to_vcf.output.tbi,
            chrom=CHROMOSOMES,
            allow_missing=True
        )
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
        "bcftools concat --threads {threads} --file-list {input.lst} --output-format z9 --output {output.vcf}"
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

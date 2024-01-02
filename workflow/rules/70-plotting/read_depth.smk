
rule plot_mosdepth_agg_window_coverage:
    input:
        tsv = rules.aggregate_mosdepth_windowed_coverage.output.tsv
    output:
        pdf = DIR_RES.joinpath(
            "plots", "read_depth", "mosdepth",
            "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}.wg-agg-{mrg_win}.pdf",
        )
    conda:
        DIR_ENVS.joinpath("plotting.yaml")
    params:
        script=find_script("plot_win_read_depth.py")
    shell:
        "{params.script} --agg-table {input.tsv} --out-pdf {output.pdf}"



if HIFI_SAMPLES:
    rule plot_agg_window_hifi_read_depth:
        input:
            pdf = expand(
                rules.plot_mosdepth_agg_window_coverage.output.pdf,
                sample=HIFI_SAMPLES,
                read_type=["hifi"],
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ,
                mrg_win=["1M", "100k"]
            )  # TODO --- mrg_win should be parameter


if ONT_SAMPLES:
    rule plot_agg_window_ont_read_depth:
        input:
            pdf = expand(
                rules.plot_mosdepth_agg_window_coverage.output.pdf,
                sample=ONT_SAMPLES,
                read_type=["ont"],
                aligner=ONT_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ,
                mrg_win=["1M", "100k"]
            )

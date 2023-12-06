
rule aggregate_mosdepth_windowed_coverage:
    input:
        bed = DIR_PROC.joinpath(
            "25-coverage", "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}",
            "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}.regions.bed.gz",
        )
    output:
        tsv = DIR_RES.joinpath(
            "read_depth", "windowed", "mosdepth",
            "{sample}_{read_type}.{aligner}.{ref}.win.mq{mapq}.wg-agg-{mrg_win}.tsv",
        )
    resources:
        mem_mb=lambda wildcards, attempt: 1024 * attempt
    params:
        merge_window=lambda wildcards: {"1M": int(1e6)}[wildcards.mrg_win]  # TODO make function
    run:
        import numpy as np
        import pandas as pd

        mrg_win = int(params.merge_window)

        def chrom_sort_order(chrom):
            try:
                order_num = int(chrom.strip("chr"))
            except ValueError:
                try:
                    order_num = {
                        "X": 23, "Y": 24,
                        "M": 25, "MT": 25
                    }[chrom.strip("chr")]
                except KeyError:
                    order_num = -1
            return order_num

        df = pd.read_csv(
            input.bed, sep="\t", header=None,
            names=["chrom", "start", "end", "coverage"]
        )
        df["sort_order"] = df["chrom"].apply(chrom_sort_order)
        df = df.loc[df["sort_order"] > -1, :].copy()
        df.sort_values(["sort_order", "start"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # stored in output
        global_median_cov = int(df["coverage"].median())
        # compute how many windows to merge/aggregate
        steps_per_window = mrg_win / (df.loc[0, "end"] - df.loc[0, "start"]))

        wg_cov = []
        for (sort_order, chrom), cov_windows in df.groupby(["sort_order", "chrom"]):
            if cov_windows.shape[0] < steps_per_window:
                continue
            # reset index to avoid offsetting cut_idx - see below
            chrom_windows = cov_windows.reset_index(drop=True, inplace=False)
            find_overhang = chrom_windows["end"].max() // mrg_win * mrg_win
            cut_idx = (chrom_windows["end"] == find_overhang).idxmax() + 1
            cov_values = chrom_windows["coverage"].values[:cut_idx]
            cov_values = np.reshape(cov_values, (-1, steps_per_window))
            cov_values.sort(axis=1)  # NB: sorts in-place
            median_cov = cov_values[:, steps_per_window//2]
            new_index = pd.MultiIndex.from_tuples(
                [
                    (chrom, sort_order, win_idx, start, end) for win_idx, start, end in
                    zip(
                        range(0,median_cov.size),
                        range(0, median_cov.size * mrg_win, mrg_win),
                        range(mrg_win, median_cov.size * mrg_win + mrg_win, mrg_win),
                    )
                ],
                names=["chrom", "sort_order", "win_idx", "start", "stop"]
            )
            # the multi-index might be a bit excessive here ...
            median_cov = pd.Series(median_cov, index=new_index, name="median_cov")
            wg_cov.append(median_cov)

        wg_cov = pd.concat(wg_cov, axis=0, ignore_index=False)

        with open(output.tsv, "w") as table:
            _ = table.write(f"#global_median_cov\t{global_median_cov}\n")
            wg_cov.to_csv(table, sep="\t", header=True, index=True)

    # END OF RUN BLOCK


if HIFI_SAMPLES:
    rule aggregate_genome_hifi_read_depth:
        input:
            agg = expand(
                rules.aggregate_mosdepth_windowed_coverage.output.tsv,
                sample=HIFI_SAMPLES,
                read_type=["hifi"],
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ,
                mrg_win=["1M"]
            )


if ONT_SAMPLES:
    rule aggregate_genome_ont_read_depth:
        input:
            agg = expand(
                rules.aggregate_mosdepth_windowed_coverage.output.tsv,
                sample=HIFI_SAMPLES,
                read_type=["ont"],
                aligner=HIFI_ALIGNER_WILDCARDS,
                ref=USE_REF_GENOMES,
                mapq=MOSDEPTH_MIN_MAPQ,
                mrg_win=["1M"]
            )

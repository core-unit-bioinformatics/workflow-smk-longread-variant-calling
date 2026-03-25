"""Module containing utility rules
to process the constant files (sample
sheet [if applicable], workflow config)
that are not handled by the 'accounting'
module.
"""


localrules:
    dump_config,


rule dump_config:
    output:
        RUN_CONFIG_RELPATH,
    params:
        acc_in=lambda wildcards, output: register_input(output, allow_non_existing=True),
    run:
        import yaml

        runinfo = {"_timestamp": get_timestamp(), "_username": get_username()}
        git_labels = collect_git_labels()
        for label, value in git_labels:
            runinfo[f"_{label}"] = value
        # add complete Snakemake config
        runinfo.update(config)
        for special_key in ["devmode", "resetacc"]:
            try:
                del runinfo[special_key]
            except KeyError:
                pass

        with open(RUN_CONFIG_RELPATH, "w", encoding="ascii") as cfg_dump:
            yaml.dump(runinfo, cfg_dump, allow_unicode=False, encoding="ascii")
        # END OF RUN BLOCK


if SAMPLE_SHEET_NAME is not None:

    localrules:
        copy_sample_sheet,

    rule copy_sample_sheet:
        input:
            SAMPLE_SHEET_PATH,
        output:
            COPY_SAMPLE_SHEET_RELPATH,
        params:
            acc_in=lambda wildcards, output: register_input(
                output, allow_non_existing=True
            ),
        shell:
            "rsync {input} {output}"

else:

    localrules:
        no_sample_sheet,

    rule no_sample_sheet:
        """This is a mock-up rule
        needed because Snakemake cannot
        handle an empty input/output rule
        that would emerge above if no
        sample sheet is provided for the
        workflow run
        """
        output:
            COPY_SAMPLE_SHEET_RELPATH,
        shell:
            "touch {output}"



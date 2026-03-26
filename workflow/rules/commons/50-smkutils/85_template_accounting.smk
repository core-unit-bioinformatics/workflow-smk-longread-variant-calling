"""Module containing some utility rules
to realize the file accounting / the
manifest creation.
"""

rule accounting_file_md5_size:
    """
    Compute MD5 checksum and file size
    for accounted files (inputs, references, results).
    For convenience, this rule also computes the file
    size to avoid this overhead at some other place
    of the pipeline.
    """
    input:
        source=load_file_by_path_id,
    output:
        md5=DIR_PROC.joinpath(
            ".accounting", "checksums", "{file_type}", "{file_name}.{path_id}.md5"
        ),
        file_size=DIR_PROC.joinpath(
            ".accounting", "file_sizes", "{file_type}", "{file_name}.{path_id}.bytes"
        ),
    benchmark:
        DIR_RSRC.joinpath(
            ".accounting", "checksums", "{file_type}", "{file_name}.{path_id}.md5.rsrc"
        )
    wildcard_constraints:
        file_type="(" + "|".join(sorted(ACCOUNTING_FILES.keys())) + ")",
    resources:
        time_hrs=lambda wildcards, attempt: 1 * attempt,
        mem_gb=lambda wildcards, attempt: 1 * attempt,
    shell:
        "md5sum {input.source} > {output.md5}"
        " && "
        "stat -c %s {input.source} > {output.file_size}"


rule accounting_file_sha256:
    """
    Compute SHA256 checksum (same as for MD5)
    """
    input:
        source=load_file_by_path_id,
    output:
        sha256=DIR_PROC.joinpath(
            ".accounting", "checksums", "{file_type}", "{file_name}.{path_id}.sha256"
        ),
    benchmark:
        DIR_RSRC.joinpath(
            ".accounting",
            "checksums",
            "{file_type}",
            "{file_name}.{path_id}.sha256.rsrc",
        )
    wildcard_constraints:
        file_type="(" + "|".join(sorted(ACCOUNTING_FILES.keys())) + ")",
    resources:
        time_hrs=lambda wildcards, attempt: 1 * attempt,
        mem_gb=lambda wildcards, attempt: 1 * attempt,
    shell:
        "sha256sum {input.source} > {output.sha256}"


rule create_manifest:
    input:
        manifest_files=load_accounting_information,
    output:
        manifest=MANIFEST_RELPATH,
    run:
        import fileinput
        import collections
        import pandas

        # The following checks if accounting files are actually
        # in use - it's possible to write a workflow w/o using
        # reference files, and thus _not all_ of them have to
        # exist / be used. Part of fix for gh#15.
        process_accounting_files = {}
        for accounting_file, file_path in ACCOUNTING_FILES.items():
            if not file_path.is_file():
                if VERBOSE:
                    warn_msg = f"Warning: accounting file of type '{account_file}' not in use."
                    logerr(warn_msg)
                continue
            process_accounting_files[accounting_file] = file_path

        accounting_files_in_use = len(process_accounting_files)

        if accounting_files_in_use == 0:
            target_rule = "run_all_no_manifest"
            if NAME_SNAKEFILE == "snaketests":
                target_rule = "run_tests_no_manifest"

            err_msg = "No accounting files marked as in use.\n"
            err_msg += "This means one of three things:\n"
            err_msg += "0) You forgot to trigger the manifest creation\n"
            err_msg += "by running Snakemake in dry run mode twice\n"
            err_msg += "before the actual pipeline run.\n"
            err_msg += "1) Your workflow does not consume input, does not use\n"
            err_msg += "any reference file(s) and also does not produce output.\n"
            err_msg += "Really? Are you sure?\n"
            err_msg += "2) You did not annotate the workflow rules with:\n"
            err_msg += "commons/02_pyutils.smk::register_input()\n"
            err_msg += "commons/02_pyutils.smk::register_result()\n"
            err_msg += "commons/02_pyutils.smk::register_reference()\n"
            err_msg += "Please rerun the workflow twice in dry run mode...\n\n"
            err_msg += "snakemake --dry-run (or: -n) [...other options...]\n\n"
            err_msg += "...after fixing that.\n\n"
            err_msg += "However, if you are sure (!) that this is correct,\n"
            err_msg += f"please target the rule >>> {target_rule} <<<\n"
            err_msg += "to run the entire workflow w/o the manifest file.\n\n"
            logerr(err_msg)
            raise RuntimeError(
                "No accounts: cannot proceed with workflow execution w/o accouting files."
            )

        if len(input.manifest_files) == 0:
            assert accounting_files_in_use > 0
            # this combination of conditions can only
            # indicate an error
            err_msg = "No files collected to list in the MANIFEST, but\n"
            err_msg += f"{accounting_files_in_use} accounting files are\n"
            err_msg += "marked as in use.\n"
            err_msg += "Please check that you properly annotated rules\n"
            err_msg += "consuming input or reference files, and rules\n"
            err_msg += "producing output with the respective 'register_'\n"
            err_msg += "function from the\n"
            err_msg += "commons::40-pyutils::85_template_refcon.smk module.\n\n"
            logerr(err_msg)
            raise RuntimeError(
                "No manifest files collected, but accounting files are in use."
            )

        records = collections.defaultdict(dict)
        for line in fileinput.input(process_accounting_files.values(), mode="r"):
            path_id, path_record = process_accounting_record(line)
            records[path_id].update(path_record)

        df = pandas.DataFrame.from_records(list(records.values()))
        if df.empty:
            logerr("Manifest DataFrame is empty - aborting")
            raise RuntimeError("Manifest DataFrame is empty")

        df.sort_values(["file_category", "file_name"], ascending=True, inplace=True)
        reordered_columns = [
            "file_name",
            "file_category",
            "file_size_bytes",
            "file_checksum_md5",
            "file_checksum_sha256",
            "file_path",
            "path_id",
        ]
        assert all(c in df.columns for c in reordered_columns)
        df = df[reordered_columns]
        df.to_csv(output.manifest, header=True, index=False, sep="\t")
    # END OF RUN BLOCK

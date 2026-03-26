"""Testing module for file I/O
operations (rsync copy/move)
"""


rule test_rsync_f2d:
    input:
        rules.create_test_file.output,
        rules.test_find_script_fail.output,
    output:
        DIR_PROC.joinpath("testing", "subfolder", "ts-ok_user-ok.txt"),
    params:
        acc_out=lambda wildcards, output: register_result(output),
    run:
        import pathlib  # workaround, see gh#20

        # first check that nobody changed the filename
        input_name = pathlib.Path(input[0]).name
        output_name = pathlib.Path(output[0]).name
        assert input_name == output_name
        output_dir = pathlib.Path(output[0]).parent
        rsync_f2d(input[0], output_dir)
        # END OF RUN BLOCK


rule test_rsync_f2f:
    input:
        rules.create_test_file.output,
    output:
        DIR_PROC.joinpath("testing", "rsync-f2f-ok.txt"),
    params:
        acc_out=lambda wildcards, output: register_result(output),
    run:
        rsync_f2f(input[0], output[0])
        # END OF RUN BLOCK



rule test_rsync_fail:
    input:
        rules.create_test_file.output,
    output:
        DIR_PROC.joinpath("testing", "rsync-fail-ok.txt"),
    message:
        "EXPECTED FAILURE: ignore following rsync error message"
    params:
        acc_out=lambda wildcards, output: register_result(output),
    run:
        import subprocess

        try:
            rsync_f2d(input[0], "/")
        except subprocess.CalledProcessError:
            with open(output[0], "w") as testfile:
                testfile.write("rsync fail test ok")
        # END OF RUN BLOCK


rule test_all_file_io:
    input:
        rules.test_rsync_f2d.output,
        rules.test_rsync_f2f.output,
        rules.test_rsync_fail.output

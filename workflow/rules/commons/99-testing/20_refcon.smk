"""Testing module that triggers
a run using reference containers, thus
implicitly testing all related functions.
"""


if USE_REFERENCE_CONTAINER:
    CONTAINER_TEST_FILES = [
        DIR_GLOBAL_REF.joinpath("genome.fasta.fai"),
        DIR_GLOBAL_REF.joinpath("exclusions.bed"),
        DIR_GLOBAL_REF.joinpath("hg38_full.fasta.fai"),
        DIR_REFCON_CACHE.joinpath("refcon_manifests.cache"),
    ]
    REGISTER_REFERENCE_FILES = CONTAINER_TEST_FILES[:3]
else:
    CONTAINER_TEST_FILES = []
    REGISTER_REFERENCE_FILES = []


rule test_refcon_functionality:
    input:
        CONTAINER_TEST_FILES,
    output:
        DIR_PROC.joinpath("testing", "refcon-ok.txt"),
    params:
        acc_out=lambda wildcards, output: register_result(output),
        acc_ref=lambda wildcards, input: register_reference(REGISTER_REFERENCE_FILES),
    run:
        with open(output[0], "w") as testfile:
            testfile.write("ok")
        # END OF RUN BLOCK

import sys

CONSTRAINT_SAMPLES = _build_constraint(SAMPLES)

# TODO infer input from known inputs
CONSTRAINT_READ_TYPE = _build_constraint(["hifi", "ont"])


# Prepare lookup structure for
# reference genomes
_user_ref_genomes = config.get("reference_genomes", None)
if _user_ref_genomes is None:
    raise ValueError(f"Config does not contain key: 'reference_genomes'")
REF_GENOMES = dict()
USE_REF_GENOMES = []
for _ref_label, _ref_file in _user_ref_genomes.items():
    _path_to_ref = DIR_GLOBAL_REF.joinpath(_ref_file)
    REF_GENOMES[_ref_label] = _path_to_ref
    _ref_suffix = _path_to_ref.suffix
    _ref_fai_suffix = f"{_ref_suffix}.fai"
    _path_to_idx = _path_to_ref.with_suffix(_ref_fai_suffix)
    REF_GENOMES[(_ref_label, "fai")] = _path_to_idx
    USE_REF_GENOMES.append(_ref_label)
CONSTRAINT_REF_GENOMES = "(" + "|".join(USE_REF_GENOMES + ["prg", "prg1", "prg2"]) + ")"


CHROMOSOMES = config.get("call_chromosomes", ["chr1"])
assert isinstance(CHROMOSOMES, list)

### if the genotyping data flow / personalized reference genome
# modules need to be executed, the PanGenie singularity container
# must be available and the "container_store" path variable
# is likely set via the environment config
# By default, just assume that the user put the container into
# the working directory
CONTAINER_STORE = pathlib.Path(config.get("container_store", WORKDIR)).resolve(strict=True)


#######
### For the time being: only used for personalized reference genome

# TODO
# potentially, this should be directly encoded via the sample
# sheet given that the case/control structure can also represented
# via the sample sheet by now

SAMPLE_PAIRS = None
PAIRED_CONTROLS = None
PAIRED_CASES = None
PAIRED_SAMPLES = None
SAMPLE_TO_PAIRING = None

_sample_pairs = config.get("sample_pairs", None)
if _sample_pairs is not None:

    SAMPLE_PAIRS = dict()
    SAMPLE_TO_PAIRING = dict()
    PAIRED_CONTROLS = []
    PAIRED_CASES = []

    for pair_label, sample_pair in _sample_pairs.items():
        assert len(sample_pair) == 2
        control_sample, case_sample = sample_pair
        pairing = {
            "control": control_sample,
            "case": case_sample,
            "label": pair_label
        }
        SAMPLE_PAIRS[pair_label] = pairing
        assert case_sample not in SAMPLE_PAIRS
        SAMPLE_PAIRS[case_sample] = control_sample
        SAMPLE_TO_PAIRING[case_sample] = pair_label
        assert control_sample not in SAMPLE_PAIRS
        SAMPLE_PAIRS[control_sample] = case_sample
        SAMPLE_TO_PAIRING[control_sample] = pair_label

        if CONTROL_SAMPLES:
            assert control_sample in CONTROL_SAMPLES
        if CASE_GROUPS:
            assert case_sample in CASE_GROUPS["all"]

        PAIRED_CONTROLS.append(control_sample)
        PAIRED_CASES.append(case_sample)

    PAIRED_SAMPLES = sorted(set(PAIRED_CASES).union(set(PAIRED_CONTROLS)))
else:
    PAIRED_SAMPLES = []


#############################
### Check if user-specified
### ROI files are available
### and match with a known
### genome reference
#############################

def process_user_roi_files(user_roi_config, ref_genome_labels):
    """Utility function to encapsulate roi file
    processing:
    - is each ROI file available in the working dir?
    - is each ROI paired with an existing genome reference?

    Return:
        list: (Snakemake) wildcards values of the form
            <REF-LABEL>.<ROI-LABEL>
        dict: lookup table to get the ROI file path by
            ROI file label
    """
    roi_file_wildcards = []
    roi_file_paths_by_label = dict()

    for roi_label, (ref_label, roi_file) in user_roi_config.items():
        if ref_label == "all":
            pair_ref_labels = ref_genome_labels
        elif ref_label == "any":
            pair_ref_labels = ref_genome_labels[0]
        elif ref_label not in ref_genome_labels:
            err_msg = (
                "ERROR processing user-specified ROI files.\n"
                f"The ROI file labeled >{roi_label}< is relative "
                f"to the genome reference labeled >{ref_label}<, "
                f"but that reference label does not exist:\n"
                f"Know reference labels: {sorted(ref_genome_labels)}\n"
            )
            raise ValueError(err_msg)
        else:
            pair_ref_labels = [ref_label]

        # Because ROI files are usually stored in the project
        # repository, the user has to copy those files to the
        # local reference folder
        # (not global s.t. reference containers can still
        # be used at the same time)
        path_to_roi = DIR_LOCAL_REF.joinpath(roi_file)
        if not path_to_roi.is_file():
            err_msg = (
                "ERROR processing user-specified ROI files.\n"
                f"The file labeled >{roi_label}< does not exist "
                f"at location: {path_to_roi}\n"
                f"(Absolute path: {path_to_roi.resolve()})\n"
                "Please copy the file to that folder."
            )
            raise ValueError(err_msg)
        if roi_label in roi_file_paths_by_label:
            err_msg = (
                f"ERROR: the ROI file label >{roi_label}< "
                "already exists and identifies this file:\n"
                f"{roi_file_paths_by_label[roi_label]}"
            )
            raise ValueError(err_msg)
        roi_file_paths_by_label[roi_label] = path_to_roi

        for pair_ref in pair_ref_labels:
            roi_file_wildcards.append(
                f"{pair_ref}.{roi_label}"
            )
    roi_file_wildcards = sorted(set(roi_file_wildcards))
    return roi_file_wildcards, roi_file_paths_by_label


USER_ROI_FILES = dict()
USER_ROI_FILE_WILDCARDS = []
_user_roi_config = config.get("user_roi", None)
if _user_roi_config is not None:
    USER_ROI_FILE_WILDCARDS, USER_ROI_FILES = process_user_roi_files(
        _user_roi_config, USE_REF_GENOMES
    )


###########################################
### SETTINGS FOR PBSV ONLY
###########################################

#RUN_PBSV_MULTISAMPLE_MODE = config.get("run_pbsv_multisample_mode", False)
#assert isinstance(RUN_PBSV_MULTISAMPLE_MODE, bool)
#PBSV_MULTISAMPLE_SETS = config.get("pbsv_multisample_sets", dict())

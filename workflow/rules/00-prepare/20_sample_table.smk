import enum
import pandas
import pathlib
import hashlib
import collections
import re

SAMPLES = None
SAMPLE_SEX = None

COHORT_SAMPLES = None
BASELINE_SAMPLES = None
CASE_GROUPS = None
CONTROL_SAMPLES = None

HIFI_SAMPLES = []
ONT_SAMPLES = []
HIFI_INPUT = []
ONT_INPUT = []

MAP_SAMPLE_TO_INPUT_FILES = None
MAP_PATHID_TO_FILE_INFO = None

MATERNAL_ID_MAP = {}
PATERNAL_ID_MAP = {}
DUO_PARENT_MAP = {}
TRIO_CHILDREN = []
DUO_CHILDREN = []
MULTI_CHILD_FAMILIES = []
SINGLE_CHILD_FAMILIES = []
FAMILY_CHILDREN = {}
FAMILY_REPRESENTATIVE_CHILD = {}


class MandatorySampleSheetColumn(enum.Enum):
    """
    Canonical sample sheet columns with aliases mapped to the same value.
    """

    # sample
    sample = 0
    sample_id = 0
    samples = 0
    sampleid = 0
    # read_type
    read_type = 1
    readtype = 1
    platform = 1
    # input_path
    input_path = 2
    input_file = 2
    fastqs = 2
    # maternal_id
    maternal_id = 3
    mother = 3
    mat_id = 3
    # paternal_id
    paternal_id = 4
    father = 4
    pat_id = 4


class VariantCallingMode(enum.Enum):
    population = 0
    trio = 1


def normalize_sample_sheet_columns(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Normalize known column names in the sample sheet to canonical names.
    """
    old_columns = df.columns
    new_columns = []

    for column in old_columns:
        col_clean = column.strip()
        col_lower = col_clean.lower()
        try:
            mandatory_column = MandatorySampleSheetColumn[col_lower]
            new_columns.append(mandatory_column.name)
        except KeyError:
            new_columns.append(column)
    df.columns = new_columns
    return df


def normalize_parental_ids(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Normalize parental ID columns if present:
    - Fill missing values with "0"
    - Strip whitespace
    - Convert empty strings to "0"
    """
    for col in (
        MandatorySampleSheetColumn.maternal_id.name,
        MandatorySampleSheetColumn.paternal_id.name,
    ):
        if col in df.columns:
            df[col] = df[col].fillna("0").astype(str).str.strip().replace({"": "0"})
    return df


def validate_sample_sheet_columns(
    df: pandas.DataFrame, mode: VariantCallingMode
) -> None:
    """
    Validate that required columns are present depending on mode:
    - population: sample, read_type, input_path
    - trio: above + maternal_id, paternal_id
    Uses Enum members instead of raw string lists.
    """
    required_population = {
        MandatorySampleSheetColumn.sample.name,
        MandatorySampleSheetColumn.read_type.name,
        MandatorySampleSheetColumn.input_path.name,
    }
    required_trio = required_population.union(
        {
            MandatorySampleSheetColumn.maternal_id.name,
            MandatorySampleSheetColumn.paternal_id.name,
        }
    )
    if mode == VariantCallingMode.population:
        missing = required_population - set(df.columns)
    elif mode == VariantCallingMode.trio:
        missing = required_trio - set(df.columns)
    else:
        raise ValueError(f"Unknown variant calling mode: {mode}")

    if missing:
        raise ValueError(
            f"Missing required columns for variant calling mode '{mode.value}': {missing}"
        )

    return


def build_trio_pedigree(sample_sheet: pandas.DataFrame):
    """
    Trio-specific:
    - Build maternal/paternal ID maps
    - Perform pedigree consistency checks
    - Return maps and list of trio children
    """
    maternal_map = {}
    paternal_map = {}

    sample_names = set(sample_sheet[MandatorySampleSheetColumn.sample.name])

    for row in sample_sheet.itertuples():
        sample = row.sample
        mother = row.maternal_id
        father = row.paternal_id

        # Rule 1: sample cannot be its own parent
        if sample == mother or sample == father:
            raise ValueError(f"Sample '{sample}' cannot be its own parent")

        # Rule 2: if a parent is non-zero, it must exist as a sample
        if mother != "0" and mother not in sample_names:
            raise ValueError(
                f"maternal_id '{mother}' for sample '{sample}' "
                "is not present as a sample in the sheet."
            )

        if father != "0" and father not in sample_names:
            raise ValueError(
                f"paternal_id '{father}' for sample '{sample}' "
                "is not present as a sample in the sheet."
            )

        maternal_map[sample] = mother
        paternal_map[sample] = father

    # critical piece of logic here
    # the 'or' makes this a "trio or duo" query;
    # changing to "and" to make this "trio" only
    # until the code path for duo support has
    # been implemented
    trio_children = [
        sample
        for sample in sample_names
        if maternal_map.get(sample, "0") != "0" and paternal_map.get(sample, "0") != "0"
    ]
    duo_children = [
        sample
        for sample in sample_names
        if (maternal_map.get(sample, "0") != "0")
        ^ (paternal_map.get(sample, "0") != "0")
    ]

    return maternal_map, paternal_map, trio_children, duo_children


def process_sample_sheet():
    """
    Main entry point:
    - Load and normalize the sample sheet
    - Determine variant calling mode (population/trio)
    - Build pedigree maps if trio mode
    - Collect input FASTQ files and hashes
    - Populate global sample lists and metadata structures
    """

    SAMPLE_SHEET_FILE = pathlib.Path(config["samples"]).resolve(strict=True)

    # Read config value as string; default to Enum name
    variant_calling_mode = config.get(
        "variant_calling_mode",
        VariantCallingMode.population.name,
    )
    try:
        mode = VariantCallingMode[variant_calling_mode]
    except KeyError:
        allowed_modes = [mode_member.name for mode_member in VariantCallingMode]
        error_message = (
            f"Invalid variant_calling_mode '{variant_calling_mode}'. "
            f"Allowed values: {allowed_modes}"
        )
        raise ValueError(error_message)

    SAMPLE_SHEET = pandas.read_csv(
        SAMPLE_SHEET_FILE,
        sep="\t",
        header=0,
        comment="#",
    )

    # Normalize columns
    SAMPLE_SHEET = normalize_sample_sheet_columns(SAMPLE_SHEET)
    SAMPLE_SHEET = normalize_parental_ids(SAMPLE_SHEET)

    # Check for parental columns
    maternal_col = MandatorySampleSheetColumn.maternal_id.name
    paternal_col = MandatorySampleSheetColumn.paternal_id.name

    has_maternal = maternal_col in SAMPLE_SHEET.columns
    has_paternal = paternal_col in SAMPLE_SHEET.columns

    # Only one of the two parental columns present → invalid
    if has_maternal != has_paternal:
        error_message = (
            f"Invalid sample sheet: only one of '{maternal_col}' or '{paternal_col}' "
            f"is present. For variant_calling_mode='{VariantCallingMode.trio.name}', "
            f"both columns must be provided."
        )
        raise ValueError(error_message)

    # User explicitly requests trio mode but columns missing
    if mode == VariantCallingMode.trio and not (has_maternal and has_paternal):
        error_message = (
            f"variant_calling_mode='{mode.name}' requires both "
            f"'{maternal_col}' and '{paternal_col}' columns in the sample sheet."
        )
        raise ValueError(error_message)

    # User is in population mode but trio columns are present → warn
    if mode == VariantCallingMode.population and has_maternal and has_paternal:
        logerr(
            f"Detected parental columns ('{maternal_col}', '{paternal_col}') in sample sheet, "
            f"but variant_calling_mode='{mode.name}'. Trio information will be ignored. "
            f"Set variant_calling_mode='{VariantCallingMode.trio.name}' to enable trio handling."
        )

    # Validate required columns for the selected mode
    validate_sample_sheet_columns(SAMPLE_SHEET, mode)

    # Trio-duo-specific pedigree building
    global MATERNAL_ID_MAP, PATERNAL_ID_MAP, TRIO_CHILDREN, DUO_CHILDREN, DUO_PARENT_MAP
    global MULTI_CHILD_FAMILIES, SINGLE_CHILD_FAMILIES, FAMILY_CHILDREN, FAMILY_REPRESENTATIVE_CHILD

    MATERNAL_ID_MAP = {}
    PATERNAL_ID_MAP = {}
    TRIO_CHILDREN = []
    DUO_CHILDREN = []
    DUO_PARENT_MAP = {}
    MULTI_CHILD_FAMILIES = []
    SINGLE_CHILD_FAMILIES = []
    FAMILY_CHILDREN = {}
    FAMILY_REPRESENTATIVE_CHILD = {}

    if mode == VariantCallingMode.trio:
        MATERNAL_ID_MAP, PATERNAL_ID_MAP, TRIO_CHILDREN, DUO_CHILDREN = (
            build_trio_pedigree(SAMPLE_SHEET)
        )
        DUO_PARENT_MAP = {
            sample: (
                MATERNAL_ID_MAP[sample]
                if MATERNAL_ID_MAP[sample] != "0"
                else PATERNAL_ID_MAP[sample]
            )
            for sample in DUO_CHILDREN
        }

        family_map = collections.defaultdict(
            lambda: {"parents": set(), "children": set()}
        )

        for child in TRIO_CHILDREN:
            mother = MATERNAL_ID_MAP[child]
            father = PATERNAL_ID_MAP[child]
            fam_id = f"{mother}_{father}"

            family_map[fam_id]["parents"].update([mother, father])
            family_map[fam_id]["children"].add(child)

        for fam_id, data in family_map.items():
            children = sorted(data["children"])
            FAMILY_CHILDREN[fam_id] = children
            FAMILY_REPRESENTATIVE_CHILD[fam_id] = children[0]

            if len(children) > 1:
                # Multi-child families: use fam_id (parents define the family)
                MULTI_CHILD_FAMILIES.append(fam_id)
            else:
                # Single-child families (pure trios): use child ID because trio outputs
                # are child-centric (DeepTrio, GLnexus naming)
                SINGLE_CHILD_FAMILIES.append(children[0])

    # Collect input files and hashes
    sample_input, path_input = collect_input_files(SAMPLE_SHEET)
    all_samples = sorted(sample_input.keys())

    global SAMPLES
    SAMPLES = all_samples

    # sample sex strongly suggested for pbcnv/hificnv,
    # may be used in future updates for DeepVariant as well
    global SAMPLE_SEX
    SAMPLE_SEX = dict()

    global COHORT_SAMPLES
    COHORT_SAMPLES = set()
    global BASELINE_SAMPLES
    BASELINE_SAMPLES = set()
    global CASE_GROUPS
    CASE_GROUPS = collections.defaultdict(set)
    global CONTROL_SAMPLES
    CONTROL_SAMPLES = set()

    for row in SAMPLE_SHEET.itertuples():
        if hasattr(row, "sex"):
            SAMPLE_SEX[row.sample] = row.sex
        else:
            SAMPLE_SEX[row.sample] = "any"

        # TODO fix via sample sheet normalizing script
        if hasattr(row, "sample_type"):
            sample_type = row.sample_type
        else:
            sample_type = "cohort"

        if sample_type == "baseline":
            BASELINE_SAMPLES.add(row.sample)
        else:
            COHORT_SAMPLES.add(row.sample)

        if hasattr(row, "sample_group"):
            sample_group = row.sample_group
            if sample_group == "control":
                CONTROL_SAMPLES.add(row.sample)
            elif sample_group == "baseline":
                BASELINE_SAMPLES.add(row.sample)
            elif sample_group == "case":
                CASE_GROUPS["all"].add(row.sample)
                if hasattr(row, "group_label"):
                    group_label = row.group_label
                    CASE_GROUPS[group_label].add(row.sample)
            else:
                raise ValueError(f"Unknown sample group value: {row}")

    assert len(COHORT_SAMPLES.intersection(BASELINE_SAMPLES)) == 0

    global MAP_SAMPLE_TO_INPUT_FILES
    MAP_SAMPLE_TO_INPUT_FILES = sample_input

    global MAP_PATHID_TO_FILE_INFO
    MAP_PATHID_TO_FILE_INFO = path_input

    global HIFI_SAMPLES
    global ONT_SAMPLES

    for sample, sample_info in sample_input.items():
        if len(sample_info["hifi"]["paths"]) > 0:
            HIFI_SAMPLES.append(sample)
        if len(sample_info["ont"]["paths"]) > 0:
            ONT_SAMPLES.append(sample)

    return


def collect_input_files(sample_sheet):

    sample_input = dict()
    path_input = dict()

    for row in sample_sheet.itertuples():
        if row.sample not in sample_input:
            sample_info = dict(
                [
                    (rt.name, {"paths": [], "path_hashes": [], "path_ids": []})
                    for rt in ReadTypes
                ]
            )
            sample_input[row.sample] = sample_info

        read_type = ReadTypes[row.read_type.lower()].name
        input_files, input_hashes, path_ids = collect_sequence_input(row.input_path)
        sample_input[row.sample][read_type]["paths"].extend(input_files)
        sample_input[row.sample][read_type]["path_hashes"].extend(input_hashes)
        sample_input[row.sample][read_type]["path_ids"].extend(path_ids)
        for path, full_hash, path_id in zip(input_files, input_hashes, path_ids):
            # TODO
            # ASM
            # Ignoring the unlikely event of a genuine hash
            # collision, this enforces the assumption of a
            # 1-to-1 mapping from sample to input file
            assert path_id not in path_input, "Hash prefix collision"
            path_input[path_id] = {
                "sample": row.sample,
                "read_type": read_type,
                "path": path,
                "path_hash": full_hash,
            }
            if read_type == "hifi":
                HIFI_INPUT.append(path_id)
            if read_type == "ont":
                ONT_INPUT.append(path_id)

    return sample_input, path_input


def subset_path(full_path):
    """This helper exists to reduce
    the absolute path to a file
    to just the file name and its
    parent.
    TODO: should be codified as part
    of the template utilities to improve
    infrastructure portability of active
    workflows
    """
    folder_name = full_path.parent.name
    file_name = full_path.name
    subset_path = f"{folder_name}/{file_name}"
    # if it so happens that the file resides
    # in a root-level location, strip off
    # leading slash
    return subset_path.strip("/")


def collect_sequence_input(path_spec):
    """
    Generic function to collect HiFi or ONT/Nanopore
    input (read) files
    """
    input_files = []
    input_hashes = []
    # for better (human) readability,
    # shorten the full sha256 hash
    # to just a prefix of 10 chars
    # to be used as "path_id"
    path_ids = []
    for sub_input in path_spec.split(","):
        input_path = pathlib.Path(sub_input).resolve(strict=True)
        if input_path.is_file():
            assert not input_path.name.endswith(".fofn"), "FOFN support not implemented"
            input_hash = hashlib.sha256(
                subset_path(input_path).encode("utf-8")
            ).hexdigest()
            input_files.append(input_path)
            input_hashes.append(input_hash)
            path_ids.append(input_hash[:10])
        elif input_path.is_dir():
            collected_files = _collect_files(input_path)
            collected_hashes = [
                hashlib.sha256(subset_path(f).encode("utf-8")).hexdigest()
                for f in collected_files
            ]
            collected_path_ids = [full_hash[:10] for full_hash in collected_hashes]
            input_files.extend(collected_files)
            input_hashes.extend(collected_hashes)
            path_ids.extend(collected_path_ids)
        else:
            raise ValueError(f"Cannot handle input: {sub_input}")

    return input_files, input_hashes, path_ids


def _collect_files(folder):

    all_files = set()
    for pattern in config["input_file_ext"]:
        pattern_files = set(folder.glob(f"**/*.{pattern}"))
        all_files = all_files.union(pattern_files)
    all_files = [f for f in sorted(all_files) if f.is_file()]
    if len(all_files) < 1:
        raise ValueError(f"No input files found underneath {folder}")
    return all_files


def _build_constraint(values):
    escaped_values = sorted(map(re.escape, map(str, values)))
    constraint = "(" + "|".join(escaped_values) + ")"
    return constraint


process_sample_sheet()

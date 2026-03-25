import pandas
import pathlib
import hashlib
import collections
import re
from enum import Enum


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


class SampleSheetColumns(Enum):
    SAMPLE = ("sample", ["sample_id", "id", "SampleID", "sampleid", "iid"])
    READ_TYPE = ("read_type", ["rtype", "readtype", "platform", "Platform"])
    INPUT_PATH = ("input_path", ["path", "input", "fastq", "Fastqs", "fastqs", "input.path"])
    MATERNAL_ID = ("maternal_id", ["mother", "mom", "mat_id", "Mat_ID", "mid"])
    PATERNAL_ID = ("paternal_id", ["father", "dad", "pat_id", "Pat_ID", "pid"])

    def matches(self, colname: str) -> bool:
        colname = colname.lower().strip()
        main, aliases = self.value
        aliases = [a.lower() for a in aliases]
        return colname == main or colname in aliases

    @staticmethod
    def _all_aliases():
        alias_map = {}
        for field in SampleSheetColumns:
            main, aliases = field.value
            alias_map[main] = aliases
        return alias_map

    @staticmethod
    def normalize_columns(df):
        rename_map = {}
        for col in df.columns:
            col_clean = col.lower().strip()
            matched = False

            for field in SampleSheetColumns:
                main, aliases = field.value
                aliases = [a.lower() for a in aliases]

                if col_clean == main or col_clean in aliases:
                    rename_map[col] = main
                    matched = True
                    break

            if not matched:
                raise ValueError(
                    f"Unrecognized column name '{col}'.\n"
                    f"Allowed columns: {[f.value[0] for f in SampleSheetColumns]}\n"
                    f"Aliases: {SampleSheetColumns._all_aliases()}"
                )

        return df.rename(columns=rename_map)


def validate_sample_sheet_columns(df, mode):
    required_population = {"sample", "read_type", "input_path"}
    required_trio = required_population.union({"maternal_id", "paternal_id"})

    if mode == "population":
        missing = required_population - set(df.columns)
    elif mode == "trio":
        missing = required_trio - set(df.columns)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    if missing:
        raise ValueError(f"Missing required columns for mode '{mode}': {missing}")

def process_sample_sheet():

    SAMPLE_SHEET_FILE = pathlib.Path(config["samples"]).resolve(strict=True)

    user_mode = config.get("mode", "population")

    SAMPLE_SHEET = pandas.read_csv(
        SAMPLE_SHEET_FILE,
        sep="\t",
        header=0,
        comment="#"
    )

    SAMPLE_SHEET = SampleSheetColumns.normalize_columns(SAMPLE_SHEET)
    for col in ["maternal_id", "paternal_id"]:
    	if col in SAMPLE_SHEET.columns:
        	SAMPLE_SHEET[col] = SAMPLE_SHEET[col].fillna("0").astype(str).str.strip()



    has_maternal = "maternal_id" in SAMPLE_SHEET.columns
    has_paternal = "paternal_id" in SAMPLE_SHEET.columns

    if has_maternal != has_paternal:
        raise ValueError(
            "Invalid sample sheet: only one of maternal_id/paternal_id is present.\n"
            "Provide both for trio mode or neither for population mode."
        )

    # If user explicitly requests trio mode but columns are missing -> error
    if user_mode == "trio" and not (has_maternal and has_paternal):
        raise ValueError(
            "Config mode='trio' but sample sheet does not contain both "
            "maternal_id and paternal_id columns."
        )

    # If user is in population mode but trio columns are present -> warn, stay in population
    if user_mode == "population" and has_maternal and has_paternal:
        print(
            "WARNING: maternal_id and paternal_id columns detected in sample sheet, "
            "but mode='population'. Running in population mode; trio information will "
            "NOT be used. Set mode='trio' in config to enable trio handling."
        )

    mode = user_mode

    validate_sample_sheet_columns(SAMPLE_SHEET, mode)

    if mode == "trio":
        global MATERNAL_ID_MAP, PATERNAL_ID_MAP
        MATERNAL_ID_MAP = {}
        PATERNAL_ID_MAP = {}

        # First pass: build maps and ensure non-empty
        for row in SAMPLE_SHEET.itertuples():
            if row.maternal_id in ["", None]:
                raise ValueError(f"Missing maternal_id for sample {row.sample}")
            if row.paternal_id in ["", None]:
                raise ValueError(f"Missing paternal_id for sample {row.sample}")

            MATERNAL_ID_MAP[row.sample] = row.maternal_id
            PATERNAL_ID_MAP[row.sample] = row.paternal_id

        sample_names = set(SAMPLE_SHEET["sample"])

        # Second pass: pedigree consistency checks
        for sample, mother in MATERNAL_ID_MAP.items():
            father = PATERNAL_ID_MAP[sample]

            # Rule 1: sample cannot be its own parent
            if sample == mother or sample == father:
                raise ValueError(f"Sample '{sample}' cannot be its own parent")

            mother_is_zero = (mother == "0")
            father_is_zero = (father == "0")

            # Rule 2: parents must be 0/0 (founder), 0/X (duo), or X/X (trio)
            # All three are allowed
            # BUT: if a parent is non-zero, it must exist as a sample
            if not mother_is_zero and mother not in sample_names:
                raise ValueError(
                    f"maternal_id '{mother}' for sample '{sample}' "
                    "is not present as a sample in the sheet."
                )

            if not father_is_zero and father not in sample_names:
                raise ValueError(
                    f"paternal_id '{father}' for sample '{sample}' "
                    "is not present as a sample in the sheet."
                )

    sample_input, path_input = collect_input_files(SAMPLE_SHEET)
    all_samples = sorted(sample_input.keys())

    global SAMPLES
    SAMPLES = all_samples

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
                    CASE_GROUPS[row.group_label].add(row.sample)
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
                [(rt.name, {
                    "paths": [],
                    "path_hashes": [],
                    "path_ids": []
                }) for rt in ReadTypes]
            )
            sample_input[row.sample] = sample_info

        read_type = ReadTypes[row.read_type.lower()].name
        input_files, input_hashes, path_ids = collect_sequence_input(row.input_path)
        sample_input[row.sample][read_type]["paths"].extend(input_files)
        sample_input[row.sample][read_type]["path_hashes"].extend(input_hashes)
        sample_input[row.sample][read_type]["path_ids"].extend(path_ids)

        for path, full_hash, path_id in zip(input_files, input_hashes, path_ids):
            assert path_id not in path_input, "Hash prefix collision"
            path_input[path_id] = {
                "sample": row.sample,
                "read_type": read_type,
                "path": path,
                "path_hash": full_hash
            }
            if read_type == "hifi":
                HIFI_INPUT.append(path_id)
            if read_type == "ont":
                ONT_INPUT.append(path_id)

    return sample_input, path_input


def subset_path(full_path):
    folder_name = full_path.parent.name
    file_name = full_path.name
    subset_path = f"{folder_name}/{file_name}"
    return subset_path.strip("/")


def collect_sequence_input(path_spec):
    input_files = []
    input_hashes = []
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
                hashlib.sha256(
                    subset_path(f).encode("utf-8")
                ).hexdigest() for f in collected_files
            ]
            collected_path_ids = [h[:10] for h in collected_hashes]
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


# Execute immediately (same as original)
if __name__ == "__main__":
    process_sample_sheet()

#process_sample_sheet()

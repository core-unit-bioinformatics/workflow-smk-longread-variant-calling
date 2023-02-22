import pandas

SAMPLES = None

HIFI_SAMPLES = []
ONT_SAMPLES = []
HIFI_INPUT = []
ONT_INPUT = []

MAP_SAMPLE_TO_INPUT_FILES = None
MAP_PATHID_TO_FILE_INFO = None

def process_sample_sheet():

    SAMPLE_SHEET_FILE = pathlib.Path(config["samples"]).resolve(strict=True)
    SAMPLE_SHEET = pandas.read_csv(
        SAMPLE_SHEET_FILE,
        sep="\t",
        header=0
    )

    # step 1: collect input file(s) per row
    # sample_input: key is sample
    # path_input: key is sha256 over file path
    sample_input, path_input = collect_input_files(SAMPLE_SHEET)
    all_samples = sorted(sample_input.keys())

    global SAMPLES
    SAMPLES = all_samples

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
        input_files, input_hashes, path_ids = collect_sequence_input(row.path)
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
            input_hash = hashlib.sha256(str(input_path).encode("utf-8")).hexdigest()
            input_files.append(input_path)
            input_hashes.append(input_hash)
            path_ids.append(input_hash[:10])
        elif input_path.is_dir():
            collected_files = _collect_files(input_path)
            collected_hashes = [
                hashlib.sha256(str(f).encode("utf-8")).hexdigest() for f in collected_files
            ]
            collected_path_ids = [
                full_hash[:10] for full_hash in collected_hashes
            ]
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

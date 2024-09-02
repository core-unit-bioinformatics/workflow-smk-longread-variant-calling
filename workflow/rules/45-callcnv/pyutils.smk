
def load_cn_aux_file(reference, sample, which):

    if which == "noise":
        cn_aux_file = pathlib.Path(config["cn_aux_files"][reference]["cn_noise"])
    elif which == "expect":
        try:
            sample_sex = SAMPLE_SEX[sample]
        except KeyError:
            sample_sex = "female"
        assert sample_sex in ["male", "female", "any"]
        cn_aux_file = pathlib.Path(config["cn_aux_files"][reference]["cn_expect"][sample_sex])
    else:
        raise ValueError(f"Unknown CN aux file requested: {which}")

    if cn_aux_file.is_file():
        # compensate for user mis-spec: stated full path
        pass
    else:
        cn_aux_file = DIR_GLOBAL_REF.joinpath(cn_aux_file)

    return cn_aux_file


def suffixed_number_to_int(number):

    factors = {
        "k": int(1e3),
        "m": int(1e6),
        "g": int(1e9)
    }

    try:
        num = int(number)
    except ValueError:
        suffix = number[-1]
        num = int(number[:1]) * factors[suffix]

    return int(num)

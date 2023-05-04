
def assemble_mosdepth_quantize_export(quant_steps, quant_names):

    _this_fun = (
        f"25-coverage::"
        "pyutils.smk::"
        "assemble_mosdepth_quantize_export"
    )

    if len(quant_steps) != len(quant_names):
        err_msg = (
            f"ERROR in {_this_fun}\n"
            "Length mismatch for MOSDEPTH parameters "
            "QUANTIZE_STEPS and QUANTIZE NAMES.\n"
            "There must be one name per step/bin.\n"
            f"{len(quant_steps)} vs {len(quant_names)}"
        )
        raise ValueError(err_msg)

    export_names = "\n"
    for pos, name in enumerate(quant_names, start=0):
        export_names += f"export MOSDEPTH_Q{pos}={name}\n"
    return export_names

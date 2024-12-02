
def get_sample_group_list(group_spec, add_cli_switch=True):

    if group_spec == "baseline":
        if not BASELINE_SAMPLES:
            warn_msg = (
                "rules::50-postcall::pyutils::load_sample_groups\n"
                "WARNING: sample group BASELINE requested, but set is empty"
            )
            logerr(warn_msg)
            if add_cli_switch:
                sample_group = ""
            else:
                sample_group = []
        else:
            if add_cli_switch:
                sample_group = "--baseline-samples " + " ".join(sorted(BASELINE_SAMPLES))
            else:
                sample_group = sorted(BASELINE_SAMPLES)
    elif group_spec == "cohort":
        # that is one of the default groups, cannot be empty
        if not COHORT_SAMPLES:
            raise RuntimeError("Defautl sample group COHORT is empty")
        if add_cli_switch:
            raise ValueError("No CLI switch configured for sample group COHORT")
        sample_group = sorted(COHORT_SAMPLES)
    elif group_spec == "control":
        # control sample have to be manually specified, this
        # may indeed be empty
        if not CONTROL_SAMPLES:
            warn_msg = (
                "rules::50-postcall::pyutils::load_sample_groups\n"
                "WARNING: sample group CONTROL requested, but set is empty"
            )
            logerr(warn_msg)
            if add_cli_switch:
                sample_group = ""
            else:
                sample_group = []
        if add_cli_switch:
            raise ValueError("No CLI switch configured for sample group CONTROL")
        sample_group = sorted(CONTROL_SAMPLES)
    elif group_spec in CASE_GROUPS:
        group_samples = sorted(CASE_GROUPS[group_spec])
        if add_cli_switch:
            sample_group = "--case-samples " + " ".join(sorted(group_samples))
        else:
            sample_group = sorted(group_samples)
    else:
        err_msg = (
            "rules::50-postcall::pyutils::load_sample_groups\n"
            f"ERROR: unknown sample group requested: {group_spec}"
        )
        raise ValueError(err_msg)

    return sample_group

import itertools
import operator


def _group_wildcards(*args):
    """ TODO: candidate for inclusion
    in snakemake template.

    args: tuple of lists of tuples
    --- one list per wildcard (key)
    --- one (key, value) tuple per value for wildcard

    Groups wildcards by key to facilitate
    custom wildcard pairings where the usual
    zip does not work
    """
    groups = collections.defaultdict(set)

    for arg in args:
        for (key, value) in arg:
            groups[key].add(value)

    return groups


def expand_hifi_reads(*args):

    grouped_values = _group_wildcards(*args)

    assert all(s in HIFI_SAMPLES for s in grouped_values["sample"])
    assert all(p in HIFI_INPUT for p in grouped_values["path_id"])

    # NB: default combining all path_ids with all
    # samples does of course not work
    special_groups = ["sample", "path_id"]

    other_group_keys = tuple(
        k for k in grouped_values.keys() if k not in special_groups
    )

    get_other = operator.itemgetter(*other_group_keys)
    if len(other_group_keys) == 1:
        iter_other_comb = itertools.product(get_other(grouped_values))
    else:
        # NB: unpacking here
        iter_other_comb = itertools.product(*get_other(grouped_values))

    expand_wildcards = []
    for combination in iter_other_comb:
        for path in grouped_values["path_id"]:
            sample = MAP_PATHID_TO_FILE_INFO[path]["sample"]
            wildcards = dict(
                (key, val) for key, val in zip(other_group_keys, combination)
            )
            wildcards["path_id"] = path
            wildcards["sample"] = sample
            expand_wildcards.append(wildcards)

    return expand_wildcards

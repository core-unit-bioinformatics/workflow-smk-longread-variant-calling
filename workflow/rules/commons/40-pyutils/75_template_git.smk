"""This module implements a few helper functions
to collect git information about the current
state / last commit / release of the workflow
directory. The main function 'collect_git_labels'
is only used as part of the template commons.
"""

def _check_git_available():
    """Check if git executable
    is available on the host system.
    """

    try:
        git_version = subprocess.check_output("git --version", shell=True)
        git_version = git_version.decode().strip()
        if VERBOSE:
            logerr(f"DEBUG: git version {git_version} detected in $PATH")
        git_in_path = True
    except subprocess.CalledProcessError:
        git_in_path = False
    return git_in_path


def _extract_git_remote():
    """
    Function to extract the (most likely) primary
    git remote name and URL (second part, split at ':').
    Applies following priority filter:

    1. github - CUBI convention
    2. githhu - CUBI convention
    3. origin - git default
    4. <other> (first in list, issues warning if VERBOSE is set)

    Returns:
        tuple of str: remote name, URL

    Raises:
        subprocess.CalledProcessError: if git executable
        is not available in PATH
        ValueError: if no git remotes are configured for the repo
    """

    remotes = []
    try:
        remotes = subprocess.check_output(
            "git remote -v", shell=True, cwd=DIR_REPOSITORY
        )
        remotes = remotes.decode().strip().split("\n")
        remotes = [tuple(r.split()) for r in remotes]
    except subprocess.CalledProcessError:
        error_msg = "ERROR:\n"
        error_msg = (
            f"Could not retrieve git remotes for repository: {DIR_REPOSITORY}\n"
            "Likely reason: this is not a git clone of the workflow repository.\n"
            "Consequence: the workflow repository information will not be recorded.\n"
            "This is strongly discouraged!"
        )
        # see gh#33 - no longer raises, although
        # this is really bad practice...
        logerr(error_msg)

    remote_name, remote_url = None, None

    if remotes:

        remote_priorities = {"github": 0, "githhu": 1, "origin": 2}

        # sort list of remotes by priority,
        # assign high rank / low priority to unexpected remotes
        remotes = sorted(
            [
                (remote_priorities.get(r[0], 10), r)
                for r in remotes
                if r[-1] == "(fetch)"
            ]
        )
        # drop priority value
        remote_name, remote_url, _ = remotes[0][1]
        remote_url = remote_url.split(":")[-1]

        if remote_name not in remote_priorities and VERBOSE:
            warning_msg = f"WARNING: unexpected git remote (name: {remote_name}) assumed to be primary."
            logerr(warning_msg)

    return remote_name, remote_url


def collect_git_labels():
    """
    Collect some basic information about the
    checked out git repository of the workflow
    """

    label_collection = {
        "git_remote": "unset-error",
        "git_url": "unset-error",
        "git_short": "unset-error",
        "git_long": "unset-error",
        "git_branch": "unset-error",
    }

    git_in_path = _check_git_available()

    if git_in_path:
        primary_remote, remote_url = _extract_git_remote()
    else:
        logerr(f"No git executable in $PATH on machine: {get_hostname()}")
        primary_remote, remote_url = None, None

    if primary_remote is not None:

        label_collection["git_remote"] = primary_remote
        label_collection["git_url"] = remote_url

        collect_options = [
            "rev-parse --short HEAD",
            "rev-parse HEAD",
            "rev-parse --abbrev-ref HEAD",
        ]
        info_labels = ["git_short", "git_long", "git_branch"]

        for option, label in zip(collect_options, info_labels):
            call = "git " + option
            try:
                # Important here to use DIR_REPOSITORY (= the git repo location)
                # and not WORK_DIR, which would be the pipeline working directory.
                # --- fixes gh#36
                out = subprocess.check_output(call, shell=True, cwd=DIR_REPOSITORY)
                out = out.decode().strip()
                assert label in label_collection
                label_collection[label] = out
            except subprocess.CalledProcessError as err:
                err_msg = f"\nERROR --- could not collect git info using call: {call}\n"
                err_msg += f"Error message: {str(err)}\n"
                err_msg += f"Call executed in path: {DIR_REPOSITORY}\n"
                logerr(err_msg)

    git_labels = [(k, v) for k, v in label_collection.items()]

    return git_labels

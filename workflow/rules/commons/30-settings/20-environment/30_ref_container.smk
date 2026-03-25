"""Module for settings related to
the use of reference containers.
Reference containers are the CUBI-specific
way for handling fixed reference data.
The template must always be developed in a
way that makes the use of reference containers
entirely optional.
"""

USE_REFERENCE_CONTAINER = config.get(
    OPTIONS.refcon.name, OPTIONS.refcon.default
)
USE_REFCON = USE_REFERENCE_CONTAINER  # shorthand
assert isinstance(USE_REFERENCE_CONTAINER, bool)

if USE_REFERENCE_CONTAINER:
    try:
        DIR_REFERENCE_CONTAINER = config[OPTIONS.refstore.name]
    except KeyError:
        raise KeyError(
            "The config option 'use_reference_container' is set to True. "
            "Consequently, the option 'reference_container_store' must be "
            "set to an existing folder on the file system containing the "
            "reference container images (*.sif files)."
        )
    else:
        DIR_REFERENCE_CONTAINER = pathlib.Path(DIR_REFERENCE_CONTAINER).resolve(
            strict=True
        )
        # if the workflow is configured to use reference container,
        # create the necessary caching folder structure.
        DIR_REFCON_CACHE = CONST_DIRS.cache_refcon
        DIR_REFCON_CACHE.mkdir(exist_ok=True, parents=True)
else:
    DIR_REFERENCE_CONTAINER = pathlib.Path("/")
    DIR_REFCON_CACHE = None

# TODO - ?
# this is technically a PATH whose definition is
# put here by contextual grouping because of the access
# to USE_REFERENCE_CONTAINER; logically, it would
# also fit in
# commons::30-settings::20-environment::05_paths.smk
# Same holds for the above DIR_REFCON_CACHE
DIR_REFCON = DIR_REFERENCE_CONTAINER  # shorthand


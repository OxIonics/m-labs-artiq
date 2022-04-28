import os

_DUMMY_VERSION = "7.unknown.beta"
# We need the double quotes to generate a valid line after
# poetry-dynamic-versioning has done a find and replace
__version__ = f"{_DUMMY_VERSION}"


def get_version():
    global __version__

    if __version__ != _DUMMY_VERSION:
        # poetry-dynamic-versioning has run and set the version correctly
        # or we've updated this below
        return __version__

    cwd = os.getcwd()
    try:
        import poetry_dynamic_versioning
        os.chdir(os.path.dirname(os.path.dirname(__file__)))
        __version__ = poetry_dynamic_versioning._get_version(poetry_dynamic_versioning.get_config(__file__))
    except Exception:
        pass
    finally:
        os.chdir(cwd)

    return __version__

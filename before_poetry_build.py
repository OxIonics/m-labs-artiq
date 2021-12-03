from os import path

import versioneer

version = versioneer.get_version()
versioneer.write_to_version_file(
    path.join(path.dirname(path.realpath(__file__)), "artiq", "_version.py"),
    version,
)

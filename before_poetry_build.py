from os import path
import string
import sys


def replace_version(version):
    file_dir = path.dirname(path.realpath(__file__))
    with open(path.join(file_dir, "artiq", "_version.py.template"), "r") as f:
        template = string.Template(f.read())

    with open(path.join(file_dir, "artiq", "_version.py"), "w") as f:
        f.write(template.substitute({"version": version}))


if __name__ == "__main__":
    replace_version(sys.argv[1])

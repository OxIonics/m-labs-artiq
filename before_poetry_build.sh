#!/usr/bin/env bash
set -eu

python before_poetry_build.py $(poetry version --short)

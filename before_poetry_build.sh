#!/usr/bin/env bash
set -eu

poetry version --short > artiq/_version.py

"""Generic tests for frontend commands."""
import platform
import subprocess
import sys
import unittest

HAS_LIBGL = platform.system() == "Windows" or subprocess.call(
    "ldconfig -p | grep libGL.so.1",
    shell=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.STDOUT,
) == 0


class TestFrontends(unittest.TestCase):
    def test_help(self):
        """Test --help as a simple smoke test against catastrophic breakage."""
        commands = {
            "aqctl": [
                "corelog", "moninj_proxy"
            ],
            "artiq": [
                "client", "compile", "coreanalyzer", "coremgmt",
                "flash", "master", "mkfs", "route",
                "rtiomon", "run", "session",
            ]
        }

        for module in (prefix + "_" + name
                       for prefix, names in commands.items()
                       for name in names):
            subprocess.check_call(
                [sys.executable, "-m", "artiq.frontend." + module, "--help"],
                stdout=subprocess.DEVNULL
            )

    @unittest.skipUnless(HAS_LIBGL, "no Graphics library")
    def test_help_guis(self):
        """Test --help as a simple smoke test against catastrophic breakage."""
        commands = [
                "artiq_browser", "artiq_dashboard"
        ]

        for module in commands:
            subprocess.check_call(
                [sys.executable, "-m", "artiq.frontend." + module, "--help"],
                stdout=subprocess.DEVNULL
            )

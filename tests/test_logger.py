# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import os
import sys
from pathlib import Path


def test_logger_coverage(tmp_path: Path) -> None:
    # Need to simulate logs dir not existing to cover line 36 of logger.py

    # Save the current cwd and change to a temp dir where 'logs' doesn't exist
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        if "coreason_etl_uspto.utils.logger" in sys.modules:
            del sys.modules["coreason_etl_uspto.utils.logger"]
        import coreason_etl_uspto.utils.logger as log_mod

        assert log_mod.logger is not None
        assert os.path.exists("logs")
    finally:
        os.chdir(old_cwd)

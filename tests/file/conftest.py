# File: conftest.py
# Description: Unit test configurations for file methods.
#
# Copyright (c) 2025 Jason Stuber
# Licensed under the MIT License. See the LICENSE file for more details.

import os
import pytest

@pytest.fixture(scope="session")
def file_test_fixtures_directory():
    """
    Find the 'tests' directory in the path hierarchy and return the path to its 'file/fixtures' subdirectory.
    """
    path = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(path) == "tests":
            return os.path.join(path, "file/fixtures")
        new_path = os.path.dirname(path)
        if new_path == path:
            raise RuntimeError("Could not find 'tests' directory in path hierarchy.")
        path = new_path

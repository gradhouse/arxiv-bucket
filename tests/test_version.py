# File: test_version.py
# Description: Unit tests for the versioning of the arxiv_bucket package.
#
# Copyright (c) 2025 Jason Stuber
# Licensed under the MIT License. See the LICENSE file for more details.

def test_version():
    """Test that the package version is correctly set."""
    import arxiv_bucket
    assert arxiv_bucket.__version__ == "0.1.0"

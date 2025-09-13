# File: test_bulk_archive_handler.py
# Description: Unit tests for the BulkArchiveHandler class.
#
# Copyright (c) 2025 Jason Stuber
# Licensed under the MIT License. See the LICENSE file for more details.
#
# Disclaimer:
# This software is not affiliated with, endorsed by, or sponsored by arXiv, Cornell University, or any of their affiliates.
# All arXiv data, logos, and trademarks are the property of their respective owners.
# Users of this software are solely responsible for ensuring their use of arXiv data complies with arXiv's policies and terms.
# For more information, see:
# - https://arxiv.org/help/license
# - https://info.arxiv.org/help/bulk_data_s3.html

import pytest
from arxiv_bucket.arxiv.bulk_archive_handler import BulkArchiveHandler
from arxiv_bucket.file.file_type import FileType
from arxiv_bucket.file.file_system import FileSystem
from arxiv_bucket.file.file_handler import FileHandler
from arxiv_bucket.services.hash_service import HashType

@pytest.mark.parametrize(
    "filename,expected",
    [
        ("arXiv_src_9902_005.tar", ("99", "02", "005")),
        ("arXiv_src_2301_001.tar", ("23", "01", "001")),
        ("arXiv_src_2007_123.tar", ("20", "07", "123")),
        ("arXiv_src_0001_000.tar", ("00", "01", "000")),
        ("arXiv_src_9912_999.tar", ("99", "12", "999")),
        ("/tmp/arXiv_src_9902_005.tar", ("99", "02", "005")),
        ("./arXiv_src_2301_001.tar", ("23", "01", "001")),
    ]
)
def test_parse_bulk_archive_filename_valid(filename, expected):
    """
    Test the BulkArchiveHandler.parse_bulk_archive_filename method with valid arXiv bulk archive filenames.
    """
    assert BulkArchiveHandler.parse_bulk_archive_filename(filename) == expected

@pytest.mark.parametrize(
    "filename",
    [
        "arXiv_src_9902_05.tar",
        "arXiv_src_9902_005.txt",
        "arXiv_src_199902_005.tar",
        "arXiv_src_9902.tar",
        "arXiv_src_9902_005",
        "src_9902_005.tar",
        "arXiv_src_9902_005.tgz",
        "randomfile.tar",
        "",
        None,
        "arXiv_src_9902_005.tar.bak",
        "arXiv_src_9902_005.tar.gz",
        "arXiv_src_9902_005.tarfoo",
        "arXiv_src_9902_005.tar123",
        "arXiv_src_9902_005.tar/",
        "arXiv_src_9902_005.tar.more",
    ]
)
def test_parse_bulk_archive_filename_invalid(filename):
    """
    Test BulkArchiveHandler.parse_bulk_archive_filename with invalid filenames.
    """
    if filename is None:
        with pytest.raises(TypeError):
            BulkArchiveHandler.parse_bulk_archive_filename(filename)
    else:
        assert BulkArchiveHandler.parse_bulk_archive_filename(filename) is None

@pytest.mark.parametrize(
    "filename,expected",
    [
        ("arXiv_src_9902_005.tar", True),
        ("arXiv_src_2301_001.tar", True),
        ("arXiv_src_2007_123.tar", True),
        ("arXiv_src_0001_000.tar", True),
        ("arXiv_src_9912_999.tar", True),
        ("/tmp/arXiv_src_9902_005.tar", True),
        ("./arXiv_src_2301_001.tar", True),
        ("arXiv_src_9913_005.tar", False),
        ("arXiv_src_9900_005.tar", False),
        ("arXiv_src_9915_005.tar", False),
        ("arXiv_src_9912_005.tar.bak", False),
        ("arXiv_src_9912_005.tar.gz", False),
        ("arXiv_src_9912_005.tarfoo", False),
        ("arXiv_src_9912_005.tar123", False),
        ("arXiv_src_9912_005.tar/", False),
        ("arXiv_src_9912_005.tar.more", False),
        ("arXiv_src_9902_05.tar", False),
        ("arXiv_src_9902_005.txt", False),
        ("arXiv_src_199902_005.tar", False),
        ("arXiv_src_9902.tar", False),
        ("arXiv_src_9902_005", False),
        ("src_9902_005.tar", False),
        ("arXiv_src_9902_005.tgz", False),
        ("randomfile.tar", False),
        ("", False),
    ]
)
def test_is_bulk_archive_filename(filename, expected):
    """
    Test BulkArchiveHandler.is_bulk_archive_filename for valid and invalid cases.
    """
    assert BulkArchiveHandler.is_bulk_archive_filename(filename) == expected

def test_is_bulk_archive_filename_empty_string():
    """
    Test that passing an empty string to is_bulk_archive_filename returns False.
    """
    assert BulkArchiveHandler.is_bulk_archive_filename("") is False

@pytest.mark.parametrize(
    "filename,expected_uri",
    [
        ("arXiv_src_9902_005.tar", "s3://arxiv/src/arXiv_src_9902_005.tar"),
        ("/tmp/arXiv_src_2301_001.tar", "s3://arxiv/src/arXiv_src_2301_001.tar"),
        ("./arXiv_src_2007_123.tar", "s3://arxiv/src/arXiv_src_2007_123.tar"),
    ]
)
def test_generate_uri_for_bulk_archive_filename_valid(filename, expected_uri):
    """
    Test that generate_uri_for_bulk_archive_filename returns the correct URI for valid filenames.
    """
    assert BulkArchiveHandler.generate_uri_for_bulk_archive_filename(filename) == expected_uri

@pytest.mark.parametrize(
    "filename",
    [
        "arXiv_src_9902_05.tar",
        "arXiv_src_9902_005.txt",
        "arXiv_src_199902_005.tar",
        "arXiv_src_9902.tar",
        "arXiv_src_9902_005",
        "src_9902_005.tar",
        "arXiv_src_9902_005.tgz",
        "randomfile.tar",
        "",
        "arXiv_src_9913_005.tar",
        "arXiv_src_9900_005.tar",
        "arXiv_src_9912_005.tar.bak",
    ]
)
def test_generate_uri_for_bulk_archive_filename_invalid(filename):
    """
    Test that generate_uri_for_bulk_archive_filename raises ValueError for invalid filenames.
    """
    with pytest.raises(ValueError):
        BulkArchiveHandler.generate_uri_for_bulk_archive_filename(filename)

@pytest.mark.parametrize(
    "file_path,expected_errors,expected_valid",
    [
        # Invalid filename pattern
        ("not_a_bulk_archive.tar", ["Filename not_a_bulk_archive.tar does not match bulk archive pattern"], False),
        # File does not exist (but pattern is valid)
        ("arXiv_src_9902_005.tar", ["File arXiv_src_9902_005.tar not found"], False),
        # Invalid extension (simulate file exists)
        ("arXiv_src_9902_005.txt", ["Filename arXiv_src_9902_005.txt does not match bulk archive pattern"], False),
    ]
)
def test_check_bulk_archive_and_is_bulk_archive_valid_basic(file_path, expected_errors, expected_valid, mocker):
    """
    Test check_bulk_archive and is_bulk_archive_valid for basic error cases.
    """
    # Mock FileSystem.is_file to simulate file existence for the second case
    if file_path == "arXiv_src_9902_005.tar":
      mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=False)
    else:
      mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)

    # Mock FileHandler methods to avoid dependency on actual files
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_ARCHIVE_TAR])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_ARCHIVE_TAR)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.list_contents", return_value=["1202.3054.gz"])

    errors = BulkArchiveHandler.check_bulk_archive(file_path)
    assert errors == expected_errors
    assert BulkArchiveHandler.is_bulk_archive_valid(file_path) == expected_valid


def test_check_bulk_archive_and_is_bulk_archive_valid_success(mocker):
    """
    Test check_bulk_archive and is_bulk_archive_valid for a fully valid bulk archive file.
    """
    file_path = "arXiv_src_9902_005.tar"
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_ARCHIVE_TAR])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_ARCHIVE_TAR)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.list_contents", return_value=["1202.3054.gz"])

    errors = BulkArchiveHandler.check_bulk_archive(file_path)
    assert errors == []
    assert BulkArchiveHandler.is_bulk_archive_valid(file_path) is True


def test_check_bulk_archive_invalid_archive_contents(mocker):
    """
    Test check_bulk_archive for a valid archive file with invalid contents.
    """
    file_path = "arXiv_src_9902_005.tar"
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_ARCHIVE_TAR])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_ARCHIVE_TAR)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])
    # Simulate invalid archive contents
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.list_contents", return_value=["not_a_submission.txt", "1202.3054.gz"])

    errors = BulkArchiveHandler.check_bulk_archive(file_path)
    assert errors == ["Archive entries do not match submission filename pattern: not_a_submission.txt"]
    assert BulkArchiveHandler.is_bulk_archive_valid(file_path) is False


def test_check_bulk_archive_extension_not_tar(mocker):
    """
    Test check_bulk_archive when the file extension is not recognized as tar.
    """
    file_path = "arXiv_src_9902_005.tar"
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    # Simulate extension is not tar
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_ARCHIVE_TAR)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.list_contents", return_value=["1202.3054.gz"])

    errors = BulkArchiveHandler.check_bulk_archive(file_path)
    assert errors == ['File extension is not tar']
    assert BulkArchiveHandler.is_bulk_archive_valid(file_path) is False


def test_check_bulk_archive_format_not_tar(mocker):
    """
    Test check_bulk_archive when the file format is not tar, even though the extension is correct.
    """
    file_path = "arXiv_src_9902_005.tar"
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    # Simulate extension is tar, but format is not tar
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_ARCHIVE_TAR])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_UNKNOWN)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.list_contents", return_value=["1202.3054.gz"])

    errors = BulkArchiveHandler.check_bulk_archive(file_path)
    assert errors == ['File format is not tar']
    assert BulkArchiveHandler.is_bulk_archive_valid(file_path) is False


@pytest.mark.parametrize(
    "file_exists, bulk_archive_errors, metadata, expected_key, expected_entry, expected_errors",
    [
        # Valid bulk archive, no errors
        (
            True,
            [],
            {
                "hash": {"SHA256": "sha256valid", "MD5": "md5valid"},
                "filename": "arXiv_src_9902_005.tar",
                "size_bytes": 1000000,
                "timestamp_iso": "2025-01-01T00:00:00+00:00"
            },
            "sha256valid",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256valid", "MD5": "md5valid"},
                    "filename": "arXiv_src_9902_005.tar",
                    "size_bytes": 1000000,
                    "timestamp_iso": "2025-01-01T00:00:00+00:00"
                },
                "origin": {
                    "uri": "s3://arxiv/src/arXiv_src_9902_005.tar"
                }
            },
            []
        ),
        # Valid bulk archive, with errors
        (
            True,
            ["File format is not tar"],
            {
                "hash": {"SHA256": "sha256error", "MD5": "md5error"},
                "filename": "arXiv_src_9902_005.tar",
                "size_bytes": 500000,
                "timestamp_iso": "2025-01-02T00:00:00+00:00"
            },
            "sha256error",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256error", "MD5": "md5error"},
                    "filename": "arXiv_src_9902_005.tar",
                    "size_bytes": 500000,
                    "timestamp_iso": "2025-01-02T00:00:00+00:00"
                },
                "origin": {
                    "uri": "s3://arxiv/src/arXiv_src_9902_005.tar"
                }
            },
            ["File format is not tar"]
        ),
        # File does not exist
        (
            False,
            [],
            {},
            None,
            None,
            []
        ),
    ]
)
def test_generate_registry_entry(
    monkeypatch,
    file_exists,
    bulk_archive_errors,
    metadata,
    expected_key,
    expected_entry,
    expected_errors
):
    """
    Test BulkArchiveHandler.generate_registry_entry for correct key/value/errors output and error handling.

    This parameterized test covers:
    - Valid bulk archives with and without errors
    - File not found error
    """

    # Patch FileSystem.is_file
    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: file_exists))

    # Patch BulkArchiveHandler.check_bulk_archive
    monkeypatch.setattr(BulkArchiveHandler, "check_bulk_archive", staticmethod(lambda path: bulk_archive_errors))

    # Patch BulkArchiveHandler.generate_uri_for_bulk_archive_filename
    monkeypatch.setattr(BulkArchiveHandler, "generate_uri_for_bulk_archive_filename", staticmethod(lambda path: "s3://arxiv/src/arXiv_src_9902_005.tar"))

    # Patch FileHandler.get_metadata
    monkeypatch.setattr(FileHandler, "get_metadata", staticmethod(lambda path, hash_types=None: metadata))

    if not file_exists:
        with pytest.raises(FileNotFoundError):
            BulkArchiveHandler.generate_registry_entry("dummy_path")
    else:
        key, entry, errors = BulkArchiveHandler.generate_registry_entry("dummy_path")
        assert key == expected_key
        assert entry == expected_entry
        assert errors == expected_errors


def test_generate_registry_entry_file_not_found():
    """
    Test generate_registry_entry when the file does not exist.
    """

    with pytest.raises(FileNotFoundError, match="File 'nonexistent_file.tar' not found."):
        BulkArchiveHandler.generate_registry_entry("nonexistent_file.tar")


def test_generate_registry_entry_invalid_filename_pattern(monkeypatch):
    """
    Test generate_registry_entry when the filename doesn't match the bulk archive pattern.
    """

    # Mock file exists
    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: True))

    # Mock check_bulk_archive to return filename pattern error
    monkeypatch.setattr(BulkArchiveHandler, "check_bulk_archive", staticmethod(lambda path: ["Filename invalid_name.tar does not match bulk archive pattern"]))

    # Mock generate_uri_for_bulk_archive_filename to raise ValueError for invalid filename
    def mock_generate_uri(path):
        raise ValueError(f"Filename {path} does not match arXiv bulk archive naming scheme")
    
    monkeypatch.setattr(BulkArchiveHandler, "generate_uri_for_bulk_archive_filename", staticmethod(mock_generate_uri))

    # Since generate_uri_for_bulk_archive_filename will raise ValueError for invalid filename,
    # the method should propagate this error
    with pytest.raises(ValueError, match="does not match arXiv bulk archive naming scheme"):
        BulkArchiveHandler.generate_registry_entry("invalid_name.tar")


def test_generate_registry_entry_hash_types_used(monkeypatch):
    """
    Test that generate_registry_entry uses the correct hash types (MD5 and SHA256).
    """

    # Mock file exists
    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: True))

    # Mock other dependencies
    monkeypatch.setattr(BulkArchiveHandler, "check_bulk_archive", staticmethod(lambda path: []))
    monkeypatch.setattr(BulkArchiveHandler, "generate_uri_for_bulk_archive_filename", staticmethod(lambda path: "s3://arxiv/src/test.tar"))

    # Mock FileHandler.get_metadata to capture the hash_types parameter
    captured_hash_types = []
    def mock_get_metadata(path, hash_types=None):
        if hash_types:
            captured_hash_types.extend(hash_types)
        return {
            "hash": {"SHA256": "test_sha256", "MD5": "test_md5"},
            "filename": "test.tar",
            "size_bytes": 1000,
            "timestamp_iso": "2025-01-01T00:00:00+00:00"
        }
    
    monkeypatch.setattr(FileHandler, "get_metadata", staticmethod(mock_get_metadata))

    # Call the method
    BulkArchiveHandler.generate_registry_entry("arXiv_src_9902_005.tar")

    # Verify the correct hash types were used
    assert HashType.HASH_TYPE_MD5 in captured_hash_types
    assert HashType.HASH_TYPE_SHA256 in captured_hash_types
    assert len(captured_hash_types) == 2


def test_generate_registry_entry_uses_sha256_as_key(monkeypatch):
    """
    Test that generate_registry_entry uses SHA256 hash as the registry key.
    """

    # Mock file exists
    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: True))

    # Mock other dependencies
    monkeypatch.setattr(BulkArchiveHandler, "check_bulk_archive", staticmethod(lambda path: []))
    monkeypatch.setattr(BulkArchiveHandler, "generate_uri_for_bulk_archive_filename", staticmethod(lambda path: "s3://arxiv/src/test.tar"))

    # Mock FileHandler.get_metadata with specific SHA256 value
    test_sha256 = "test_sha256_value_12345"
    monkeypatch.setattr(FileHandler, "get_metadata", staticmethod(lambda path, hash_types=None: {
        "hash": {"SHA256": test_sha256, "MD5": "test_md5"},
        "filename": "test.tar",
        "size_bytes": 1000,
        "timestamp_iso": "2025-01-01T00:00:00+00:00"
    }))

    # Call the method
    key, entry, errors = BulkArchiveHandler.generate_registry_entry("arXiv_src_9902_005.tar")

    # Verify the registry key is the SHA256 hash
    assert key == test_sha256

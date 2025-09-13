# File: test_submission_handler.py
# Description: Unit tests for the SubmissionHandler class.
#
# Copyright (c) 2025 Jason Stuber
# Licensed under the MIT License. See the LICENSE file for more details.

import pytest
from arxiv_bucket.arxiv.submission_handler import SubmissionHandler
from arxiv_bucket.arxiv.submission_type import SubmissionType
from arxiv_bucket.file.file_handler import FileHandler
from arxiv_bucket.file.file_type import FileType
from arxiv_bucket.file.file_system import FileSystem
from arxiv_bucket.file.handler.archive_handler import ArchiveHandler


@pytest.mark.parametrize(
    "file_list, mock_types, expected_type",
    [
        # Only PostScript file
        (
            ["paper.ps"],
            {"paper.ps": [FileType.FILE_TYPE_POSTSCRIPT_PS]},
            SubmissionType.SUBMISSION_TYPE_POSTSCRIPT,
        ),
        # Only PDF file
        (
                ["paper.pdf"],
                {"paper.pdf": [FileType.FILE_TYPE_PDF]},
                SubmissionType.SUBMISSION_TYPE_PDF,
        ),
        # Only TeX main file
        (
            ["main.tex"],
            {"main.tex": [FileType.FILE_TYPE_TEX_TEX]},
            SubmissionType.SUBMISSION_TYPE_TEX,
        ),
        # TeX main file and supporting files
        (
            ["main.tex", "fig1.png", "refs.bib"],
            {
                "main.tex": [FileType.FILE_TYPE_TEX_TEX],
                "fig1.png": [FileType.FILE_TYPE_IMAGE_PNG],
                "refs.bib": [FileType.FILE_TYPE_TEX_BIB],
            },
            SubmissionType.SUBMISSION_TYPE_TEX,
        ),
        # TeX file and unknown file
        (
            ["main.tex", "malware.exe"],
            {
                "main.tex": [FileType.FILE_TYPE_TEX_TEX],
                "malware.exe": [],
            },
            SubmissionType.SUBMISSION_TYPE_UNKNOWN,
        ),
        # Only unknown file
        (
            ["random.xyz"],
            {"random.xyz": []},
            SubmissionType.SUBMISSION_TYPE_UNKNOWN,
        ),
        # TeX file and non-supporting known file
        (
            ["main.tex", "readme.md"],
            {
                "main.tex": [FileType.FILE_TYPE_TEX_TEX],
                "readme.md": [FileType.FILE_TYPE_UNKNOWN],
            },
            SubmissionType.SUBMISSION_TYPE_UNKNOWN,
        ),
    ]
)
def test_get_submission_type_using_extension(monkeypatch, file_list, mock_types, expected_type):
    """
    Test SubmissionHandler.get_submission_type_using_extension with various file lists.

    This parameterized test checks that the method correctly determines the submission type
    based on the provided list of filenames and their associated file types. It uses monkeypatching
    to mock FileHandler.get_file_type_from_extension, allowing control over the file type detection.

    Scenarios tested include:
    - Only PostScript files (should return SUBMISSION_TYPE_POSTSCRIPT)
    - Only PDF files (should return SUBMISSION_TYPE_PDF)
    - Only TeX main files or TeX with supporting files (should return SUBMISSION_TYPE_TEX)
    - TeX files with unknown or unsupported files (should return SUBMISSION_TYPE_UNKNOWN)
    - Only unknown or unsupported files (should return SUBMISSION_TYPE_UNKNOWN)
    """

    def mock_get_file_type_from_extension(filename):
        return mock_types.get(filename, [])

    monkeypatch.setattr(FileHandler, "get_file_type_from_extension", staticmethod(mock_get_file_type_from_extension))

    result = SubmissionHandler.get_submission_type_using_extension(file_list)
    assert result == expected_type

@pytest.mark.parametrize(
    "file_exists, submission_errors, metadata, archive_contents, expected_type, expected_key, expected_entry, expected_errors",
    [
        # PDF file, no errors
        (
            True,
            [],
            {
                "hash": {"SHA256": "sha256pdf"},
                "file_type": FileType.FILE_TYPE_PDF.value,
                "other": "meta"
            },
            None,
            SubmissionType.SUBMISSION_TYPE_PDF,
            "sha256pdf",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256pdf"},
                    "file_type": FileType.FILE_TYPE_PDF.value,
                    "other": "meta",
                    "submission_type_by_extension": "PDF"
                },
                "origin": {
                    "url": "http://example.com/submission",
                    "bulk_archive_key": "bulkhash"
                }
            },
            []
        ),
        # PDF file, with errors
        (
            True,
            ["bad format"],
            {
                "hash": {"SHA256": "sha256pdferr"},
                "file_type": FileType.FILE_TYPE_PDF.value,
                "other": "meta"
            },
            None,
            SubmissionType.SUBMISSION_TYPE_UNKNOWN,
            "sha256pdferr",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256pdferr"},
                    "file_type": FileType.FILE_TYPE_PDF.value,
                    "other": "meta",
                    "submission_type_by_extension": "UNKNOWN"
                },
                "origin": {
                    "url": "http://example.com/submission",
                    "bulk_archive_key": "bulkhash"
                }
            },
            ["bad format"]
        ),
        # GZ archive, no errors, TeX contents
        (
            True,
            [],
            {
                "hash": {"SHA256": "sha256gz"},
                "file_type": FileType.FILE_TYPE_ARCHIVE_GZ.value,
                "other": "meta"
            },
            ["main.tex", "refs.bib"],
            SubmissionType.SUBMISSION_TYPE_TEX,
            "sha256gz",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256gz"},
                    "file_type": FileType.FILE_TYPE_ARCHIVE_GZ.value,
                    "other": "meta",
                    "submission_type_by_extension": "TEX"
                },
                "origin": {
                    "url": "http://example.com/submission",
                    "bulk_archive_key": "bulkhash"
                }
            },
            []
        ),
        # GZ archive, no errors, unknown contents
        (
            True,
            [],
            {
                "hash": {"SHA256": "sha256gzunk"},
                "file_type": FileType.FILE_TYPE_ARCHIVE_GZ.value,
                "other": "meta"
            },
            ["virus.exe"],
            SubmissionType.SUBMISSION_TYPE_UNKNOWN,
            "sha256gzunk",
            {
                "metadata": {
                    "hash": {"SHA256": "sha256gzunk"},
                    "file_type": FileType.FILE_TYPE_ARCHIVE_GZ.value,
                    "other": "meta",
                    "submission_type_by_extension": "UNKNOWN"
                },
                "origin": {
                    "url": "http://example.com/submission",
                    "bulk_archive_key": "bulkhash"
                }
            },
            ["Unknown submission type"]
        ),
        # File does not exist
        (
            False,
            [],
            {},
            None,
            None,
            None,
            None,
            []
        ),
    ]
)
def test_generate_registry_entry(
    monkeypatch,
    file_exists,
    submission_errors,
    metadata,
    archive_contents,
    expected_type,
    expected_key,
    expected_entry,
    expected_errors
):
    """
    Test SubmissionHandler.generate_registry_entry for correct key/value/errors output and error handling.

    This parameterized test covers:
    - PDF files with and without errors
    - Archive files with TeX or unknown contents
    - File not found error
    """

    # Patch FileSystem.is_file
    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: file_exists))

    # Patch Patterns.check_submission
    monkeypatch.setattr(SubmissionHandler, "check_submission", staticmethod(lambda path: submission_errors))

    # Patch Patterns.generate_url_for_submission_filename
    monkeypatch.setattr(SubmissionHandler, "generate_url_for_submission_filename", staticmethod(lambda path: "http://example.com/submission"))

    # Patch FileHandler.get_metadata
    monkeypatch.setattr(FileHandler, "get_metadata", staticmethod(lambda path, hash_types=None: metadata))

    # Patch ArchiveHandler.list_contents if archive_contents is not None
    if archive_contents is not None:
        monkeypatch.setattr(ArchiveHandler, "list_contents", staticmethod(lambda path: archive_contents))

    if not file_exists:
        with pytest.raises(FileNotFoundError):
            SubmissionHandler.generate_registry_entry("dummy_path", "bulkhash")
    else:
        key, entry, errors = SubmissionHandler.generate_registry_entry("dummy_path", "bulkhash")
        assert key == expected_key
        assert entry == expected_entry
        assert errors == expected_errors

def test_generate_registry_entry_else_submission_type_unknown(monkeypatch):
    """
    Covers the 'else' branch where the file is not an archive or PDF,
    there are no submission errors, and the file type is not allowed,
    resulting in submission_type = SUBMISSION_TYPE_UNKNOWN.
    """
    file_exists = True
    submission_errors = []
    # Use a file type that is not PDF, GZ, or TGZ
    dummy_file_type = "SOMETHING_UNSUPPORTED"
    metadata = {
        "hash": {"SHA256": "sha256dummy"},
        "file_type": dummy_file_type,
        "other": "meta"
    }
    expected_key = "sha256dummy"
    expected_entry = {
        "metadata": {
            "hash": {"SHA256": "sha256dummy"},
            "file_type": dummy_file_type,
            "other": "meta",
            "submission_type_by_extension": "UNKNOWN"
        },
        "origin": {
            "url": "http://example.com/submission",
            "bulk_archive_key": "bulkhash"
        }
    }
    expected_errors = ["Unknown submission type"]

    monkeypatch.setattr(FileSystem, "is_file", staticmethod(lambda path: file_exists))
    # Patterns.check_submission returns no errors
    monkeypatch.setattr(SubmissionHandler, "check_submission", staticmethod(lambda path: submission_errors))
    monkeypatch.setattr(SubmissionHandler, "generate_url_for_submission_filename", staticmethod(lambda path: "http://example.com/submission"))
    monkeypatch.setattr(FileHandler, "get_metadata", staticmethod(lambda path, hash_types=None: metadata))

    key, entry, errors = SubmissionHandler.generate_registry_entry("dummy_path", "bulkhash")
    assert key == expected_key
    assert entry == expected_entry
    assert errors == expected_errors

@pytest.mark.parametrize(
    "filename,expected",
    [
        # Valid old-style filenames
        ("cond-mat9602101.gz", ("cond-mat", "96", "02", "101")),
        ("hep-th9911123.pdf", ("hep-th", "99", "11", "123")),
        ("math0503123.gz", ("math", "05", "03", "123")),
        ("astro-ph0001001.pdf", ("astro-ph", "00", "01", "001")),
        ("/tmp/cond-mat9602101.gz", ("cond-mat", "96", "02", "101")),
        ("./hep-th9911123.pdf", ("hep-th", "99", "11", "123")),
    ]
)
def test_parse_old_style_submission_filename_valid(filename, expected):
    assert SubmissionHandler.parse_old_style_submission_filename(filename) == expected

@pytest.mark.parametrize(
    "filename",
    [
        "cond-mat9602101.txt",
        "hep-th9911123.doc",
        "cond-mat96021.gz",
        "cond-mat9602101",
        "cond-mat9602_101.gz",
        "cond-mat9602101.gz.bak",
        "cond-mat9602101gz",
        "cond-mat96021345.gz",
        "cond-mat9602.gz",
        "cond-mat9602101.",
        "",
        None,
    ]
)
def test_parse_old_style_submission_filename_invalid(filename):
    if filename is None:
        with pytest.raises(TypeError):
            SubmissionHandler.parse_old_style_submission_filename(filename)
    else:
        assert SubmissionHandler.parse_old_style_submission_filename(filename) is None

@pytest.mark.parametrize(
    "filename,expected",
    [
        ("1202.3054.gz", ("12", "02", "3054")),
        ("9912.12345.pdf", ("99", "12", "12345")),
        ("0001.0001.gz", ("00", "01", "0001")),
        ("2307.54321.pdf", ("23", "07", "54321")),
        ("/tmp/1202.3054.gz", ("12", "02", "3054")),
        ("./9912.12345.pdf", ("99", "12", "12345")),
    ]
)
def test_parse_current_style_submission_filename_valid(filename, expected):
    assert SubmissionHandler.parse_current_style_submission_filename(filename) == expected

@pytest.mark.parametrize(
    "filename",
    [
        "1202.3054.txt",
        "9912.12345.doc",
        "1202-3054.gz",
        "12023054.gz",
        "1202.305.gz",
        "1202.305456.gz",
        "1202.3054",
        "1202.3054.gz.bak",
        "1202.3054gz",
        "1202.3054.",
        "",
        None,
    ]
)
def test_parse_current_style_submission_filename_invalid(filename):
    if filename is None:
        with pytest.raises(TypeError):
            SubmissionHandler.parse_current_style_submission_filename(filename)
    else:
        assert SubmissionHandler.parse_current_style_submission_filename(filename) is None

@pytest.mark.parametrize(
    "filename,expected",
    [
        ("cond-mat9602101.gz", True),
        ("hep-th9911123.pdf", True),
        ("math0503123.gz", True),
        ("astro-ph0001001.pdf", True),
        ("/tmp/cond-mat9602101.gz", True),
        ("./hep-th9911123.pdf", True),
        ("1202.3054.gz", True),
        ("9912.12345.pdf", True),
        ("0001.0001.gz", True),
        ("2307.54321.pdf", True),
        ("/tmp/1202.3054.gz", True),
        ("./9912.12345.pdf", True),
        ("cond-mat9602101.txt", False),
        ("hep-th9911123.doc", False),
        ("cond-mat96021.gz", False),
        ("cond-mat9602101", False),
        ("cond-mat9602_101.gz", False),
        ("cond-mat9602101.gz.bak", False),
        ("cond-mat9602101gz", False),
        ("cond-mat96021345.gz", False),
        ("cond-mat9602.gz", False),
        ("cond-mat9602101.", False),
        ("1202.3054.txt", False),
        ("9912.12345.doc", False),
        ("1202-3054.gz", False),
        ("12023054.gz", False),
        ("1202.305.gz", False),
        ("1202.305456.gz", False),
        ("1202.3054", False),
        ("1202.3054.gz.bak", False),
        ("1202.3054gz", False),
        ("1202.3054.", False),
        ("", False),
    ]
)
def test_is_submission_filename(filename, expected):
    assert SubmissionHandler.is_submission_filename(filename) == expected

def test_is_submission_filename_none():
    with pytest.raises(TypeError):
        SubmissionHandler.is_submission_filename(None)  # type: ignore

@pytest.mark.parametrize(
    "filename,expected_url",
    [
        ("cond-mat9602101.gz", "https://arxiv.org/abs/cond-mat/9602101"),
        ("hep-th9911123.pdf", "https://arxiv.org/abs/hep-th/9911123"),
        ("math0503123.gz", "https://arxiv.org/abs/math/0503123"),
        ("astro-ph0001001.pdf", "https://arxiv.org/abs/astro-ph/0001001"),
        ("/tmp/cond-mat9602101.gz", "https://arxiv.org/abs/cond-mat/9602101"),
        ("./hep-th9911123.pdf", "https://arxiv.org/abs/hep-th/9911123"),
        ("1202.3054.gz", "https://arxiv.org/abs/1202.3054"),
        ("9912.12345.pdf", "https://arxiv.org/abs/9912.12345"),
        ("0001.0001.gz", "https://arxiv.org/abs/0001.0001"),
        ("2307.54321.pdf", "https://arxiv.org/abs/2307.54321"),
        ("/tmp/1202.3054.gz", "https://arxiv.org/abs/1202.3054"),
        ("./9912.12345.pdf", "https://arxiv.org/abs/9912.12345"),
    ]
)
def test_generate_url_for_submission_filename_valid(filename, expected_url):
    assert SubmissionHandler.generate_url_for_submission_filename(filename) == expected_url

@pytest.mark.parametrize(
    "filename",
    [
        "cond-mat9602101.txt",
        "hep-th9911123.doc",
        "cond-mat96021.gz",
        "cond-mat9602101",
        "cond-mat9602_101.gz",
        "cond-mat9602101.gz.bak",
        "cond-mat9602101gz",
        "cond-mat96021345.gz",
        "cond-mat9602.gz",
        "cond-mat9602101.",
        "1202.3054.txt",
        "9912.12345.doc",
        "1202-3054.gz",
        "12023054.gz",
        "1202.305.gz",
        "1202.305456.gz",
        "1202.3054",
        "1202.3054.gz.bak",
        "1202.3054gz",
        "1202.3054.",
        "",
        None,
    ]
)
def test_generate_url_for_submission_filename_invalid(filename):
    if filename is None:
        with pytest.raises(TypeError):
            SubmissionHandler.generate_url_for_submission_filename(filename)
    else:
        with pytest.raises(ValueError):
            SubmissionHandler.generate_url_for_submission_filename(filename)

@pytest.mark.parametrize(
    "file_path,pattern_valid,file_exists,ext_types,format_type,archive_errors,expected_errors,expected_valid",
    [
        # Invalid filename pattern
        ("not_a_submission.pdf", False, True, [FileType.FILE_TYPE_PDF], FileType.FILE_TYPE_PDF, [], ["Filename not_a_submission.pdf does not match submission pattern"], False),
        # File does not exist
        ("1202.3051.pdf", True, False, [FileType.FILE_TYPE_PDF], FileType.FILE_TYPE_PDF, [], ["File 1202.3051.pdf not found"], False),
        # Extension not allowed
        ("1202.3052.txt", True, True, [], FileType.FILE_TYPE_PDF, [], ["File extension type is not allowed"], False),
        # Format not allowed
        ("1202.3053.pdf", True, True, [FileType.FILE_TYPE_PDF], FileType.FILE_TYPE_XML, [], ["File type XML not allowed"], False),
        # Format does not match extension
        ("1202.3054.pdf", True, True, [FileType.FILE_TYPE_PDF], FileType.FILE_TYPE_ARCHIVE_GZ, [], ["File format does not match file extension"], False),
        # Archive extraction errors
        ("1202.3055.gz", True, True, [FileType.FILE_TYPE_ARCHIVE_GZ], FileType.FILE_TYPE_ARCHIVE_GZ, ["archive error"], ["archive error"], False),
        # All valid (PDF)
        ("1202.3056.pdf", True, True, [FileType.FILE_TYPE_PDF], FileType.FILE_TYPE_PDF, [], [], True),
        # All valid (GZ)
        ("1202.3057.gz", True, True, [FileType.FILE_TYPE_ARCHIVE_GZ], FileType.FILE_TYPE_ARCHIVE_GZ, [], [], True),
    ]
)
def test_check_submission_and_is_submission_valid(
    mocker, file_path, pattern_valid, file_exists, ext_types, format_type, archive_errors, expected_errors, expected_valid
):
    """
    Test check_submission and is_submission_valid for various scenarios.
    """
    # Mock SubmissionHandler.is_submission_filename
    mocker.patch("arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename", return_value=pattern_valid)
    # Mock FileSystem.is_file
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=file_exists)
    # Mock FileHandler.get_file_type_from_extension
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=ext_types)
    # Mock FileHandler.get_file_type_from_format
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=format_type)
    # Mock ArchiveHandler.check_extract_possible
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=archive_errors)

    errors = SubmissionHandler.check_submission(file_path)
    assert errors == expected_errors
    assert SubmissionHandler.is_submission_valid(file_path) == expected_valid

def test_check_submission_archive_extraction_only_if_archive(mocker):
    """
    Test that archive extraction errors are only checked for allowed archive types.
    """
    file_path = "1202.3054.pdf"
    mocker.patch("arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename", return_value=True)
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_PDF])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_PDF)
    # Should not call check_extract_possible for PDF
    mock_check_extract = mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=["archive error"])

    errors = SubmissionHandler.check_submission(file_path)
    assert errors == []
    assert SubmissionHandler.is_submission_valid(file_path) is True
    mock_check_extract.assert_not_called()

def test_check_submission_stops_on_filename_error(mocker):
    """
    Test that check_submission stops checking if filename pattern is invalid.
    """
    file_path = "invalid_filename.pdf"
    mocker.patch("arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename", return_value=False)
    # Should not call file existence check if filename is invalid
    mock_is_file = mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    mock_get_ext = mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_PDF])
    mock_get_format = mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_PDF)

    errors = SubmissionHandler.check_submission(file_path)
    assert errors == ["Filename invalid_filename.pdf does not match submission pattern"]
    assert SubmissionHandler.is_submission_valid(file_path) is False
    
    # Should not call subsequent checks when filename is invalid
    mock_is_file.assert_not_called()
    mock_get_ext.assert_not_called()
    mock_get_format.assert_not_called()

def test_check_submission_stops_on_file_not_found(mocker):
    """
    Test that check_submission stops checking if file does not exist.
    """
    file_path = "1202.3054.pdf"
    mocker.patch("arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename", return_value=True)
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=False)
    # Should not call file type checks if file doesn't exist
    mock_get_ext = mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_PDF])
    mock_get_format = mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_PDF)

    errors = SubmissionHandler.check_submission(file_path)
    assert errors == ["File 1202.3054.pdf not found"]
    assert SubmissionHandler.is_submission_valid(file_path) is False
    
    # Should not call file type checks when file doesn't exist
    mock_get_ext.assert_not_called()
    mock_get_format.assert_not_called()

def test_check_submission_tgz_archive_type(mocker):
    """
    Test that check_submission works correctly with TGZ archive files.
    """
    file_path = "1202.3054.tgz"
    mocker.patch("arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename", return_value=True)
    mocker.patch("arxiv_bucket.file.file_system.FileSystem.is_file", return_value=True)
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_extension", return_value=[FileType.FILE_TYPE_ARCHIVE_TGZ])
    mocker.patch("arxiv_bucket.file.file_handler.FileHandler.get_file_type_from_format", return_value=FileType.FILE_TYPE_ARCHIVE_TGZ)
    mocker.patch("arxiv_bucket.file.handler.archive_handler.ArchiveHandler.check_extract_possible", return_value=[])

    errors = SubmissionHandler.check_submission(file_path)
    assert errors == []
    assert SubmissionHandler.is_submission_valid(file_path) is True
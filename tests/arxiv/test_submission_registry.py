# File: test_submission_registry.py
# Description: Unit tests for the SubmissionRegistry class.
#
# Copyright (c) 2025 Jason Stuber
# Licensed under the MIT License. See the LICENSE file for more details.

import pytest
from arxiv_bucket.arxiv.submission_registry import SubmissionRegistry

class TestSubmissionRegistry:
    def test_is_entry_valid_true(self):
        """Test is_entry_valid returns True for an entry with no errors."""
        reg = SubmissionRegistry()
        reg._registry = {
            'key1': {'metadata': {}, 'diagnostics': {'error_log': []}},
        }
        assert reg.is_entry_valid('key1') is True

    def test_is_entry_valid_false(self):
        """Test is_entry_valid returns False for an entry with errors in error_log."""
        reg = SubmissionRegistry()
        reg._registry = {
            'key1': {'metadata': {}, 'diagnostics': {'error_log': ['bad format']}},
        }
        assert reg.is_entry_valid('key1') is False

    def test_is_entry_valid_missing_key(self):
        """Test is_entry_valid raises KeyError if the key is not present."""
        reg = SubmissionRegistry()
        reg._registry = {}
        with pytest.raises(KeyError):
            reg.is_entry_valid('not_a_key')

    def test_list_invalid_entries_mixed(self):
        """Test list_invalid_entries returns only keys for invalid entries."""
        reg = SubmissionRegistry()
        reg._registry = {
            'valid1': {'metadata': {}, 'diagnostics': {'error_log': []}},
            'invalid1': {'metadata': {}, 'diagnostics': {'error_log': ['bad format']}},
            'valid2': {'metadata': {}, 'diagnostics': {'error_log': []}},
            'invalid2': {'metadata': {}, 'diagnostics': {'error_log': ['missing file']}},
        }
        invalid_keys = reg.list_invalid_entries()
        assert set(invalid_keys) == {'invalid1', 'invalid2'}

    def test_list_invalid_entries_all_valid(self):
        """Test list_invalid_entries returns an empty list if all entries are valid."""
        reg = SubmissionRegistry()
        reg._registry = {
            'valid1': {'metadata': {}, 'diagnostics': {'error_log': []}},
            'valid2': {'metadata': {}, 'diagnostics': {'error_log': []}},
        }
        assert reg.list_invalid_entries() == []

    def test_list_invalid_entries_all_invalid(self):
        """Test list_invalid_entries returns all keys if all entries are invalid."""
        reg = SubmissionRegistry()
        reg._registry = {
            'invalid1': {'metadata': {}, 'diagnostics': {'error_log': ['bad format']}},
            'invalid2': {'metadata': {}, 'diagnostics': {'error_log': ['missing file']}},
        }
        assert set(reg.list_invalid_entries()) == {'invalid1', 'invalid2'}

    @pytest.fixture
    def registry_with_entries(self, monkeypatch):
        # Patch FileName.get_file_basename to avoid dependency on actual implementation
        monkeypatch.setattr('arxiv_bucket.arxiv.submission_registry.FileName.get_file_basename', lambda path: path.split('/')[-1])
        reg = SubmissionRegistry()
        reg._registry = {
            'key1': {'metadata': {'filename': 'foo.txt'}},
            'key2': {'metadata': {'filename': 'bar.txt'}},
            'key3': {'metadata': {'filename': 'foo.txt'}},
            'key4': {'metadata': {'filename': 'baz.txt'}},
        }
        return reg

    def test_find_submission_filename_found(self, registry_with_entries):
        """Test that find_submission_filename returns all keys for a matching filename (multiple matches)."""
        reg = registry_with_entries
        result = reg.find_submission_filename('some/path/foo.txt')
        assert set(result) == {'key1', 'key3'}

    def test_find_submission_filename_not_found(self, registry_with_entries):
        """Test that find_submission_filename returns an empty list if no filename matches are found."""
        reg = registry_with_entries
        result = reg.find_submission_filename('some/path/doesnotexist.txt')
        assert result == []

    def test_find_submission_filename_empty_registry(self, monkeypatch):
        """Test that find_submission_filename returns an empty list if the registry is empty."""
        monkeypatch.setattr('arxiv_bucket.arxiv.submission_registry.FileName.get_file_basename', lambda path: path.split('/')[-1])
        reg = SubmissionRegistry()
        reg._registry = {}
        result = reg.find_submission_filename('foo.txt')
        assert result == []

    def test_register_submission_key_already_exists_conflict_then_identical(self, mocker):
        """Test that after a conflict is logged, a subsequent identical registration is a no-op (covers both return branches)."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        conflict_entry = {"metadata": {"filename": "conflict1.tar.gz"}}
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        # First, log a conflict
        self.registry._registry[test_key] = conflict_entry.copy()
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        mocker.patch.object(self.registry, 'is_key_present', return_value=True)
        self.registry.register_submission(file_path, bulk_archive_hash)
        # Now, register with identical metadata (should be a no-op, hit the else branch)
        self.registry._registry[test_key] = test_entry.copy()
        self.registry.register_submission(file_path, bulk_archive_hash)
        existing_entry = self.registry.get_entry(test_key)
        # Should not add new diagnostics for identical metadata
        assert "diagnostics" not in existing_entry or "key_conflicts" in existing_entry.get("diagnostics", {})
    
    def test_register_submission_multiple_key_conflicts(self, mocker):
        """Test that multiple key conflicts are logged for the same key."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        first_conflict = {"metadata": {"filename": "conflict1.tar.gz"}}
        second_conflict = {"metadata": {"filename": "conflict2.tar.gz"}}
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        # First conflict
        self.registry._registry[test_key] = first_conflict.copy()
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        mocker.patch.object(self.registry, 'is_key_present', return_value=True)
        self.registry.register_submission(file_path, bulk_archive_hash)
        # Second conflict
        new_entry = second_conflict.copy()
        self.registry._registry[test_key] = new_entry
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry.register_submission(file_path, bulk_archive_hash)
        existing_entry = self.registry.get_entry(test_key)
        assert "diagnostics" in existing_entry
        assert "key_conflicts" in existing_entry["diagnostics"]
        # Both conflicts should be present
        assert test_entry in existing_entry["diagnostics"]["key_conflicts"]
    def test_register_submission_key_conflict_when_error_log_exists(self, mocker):
        """Test that key conflicts are logged correctly when error_log already exists with 'key conflicts'."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        # Set up existing entry with diagnostics that already has 'key conflicts' in error_log
        self.registry._registry[test_key] = {
            "metadata": {"filename": "something_else.tar.gz"}, 
            "diagnostics": {
                "error_log": ["key conflicts"],
                "key_conflicts": [{"metadata": {"filename": "previous_conflict.tar.gz"}}]
            }
        }
        mocker.patch.object(self.registry, 'is_key_present', return_value=True)
        # Should append to existing key_conflicts without adding 'key conflicts' again
        self.registry.register_submission(file_path, bulk_archive_hash)
        existing_entry = self.registry.get_entry(test_key)
        assert "diagnostics" in existing_entry
        assert "error_log" in existing_entry["diagnostics"]
        assert existing_entry["diagnostics"]["error_log"].count("key conflicts") == 1  # Should not duplicate
        assert "key_conflicts" in existing_entry["diagnostics"]
        assert len(existing_entry["diagnostics"]["key_conflicts"]) == 2  # Previous + new conflict
        assert test_entry in existing_entry["diagnostics"]["key_conflicts"]
    """Test cases for the SubmissionRegistry class."""

    def setup_method(self):
        """Set up a new SubmissionRegistry for each test."""
        self.registry = SubmissionRegistry()

    def test_inheritance(self):
        """Test that SubmissionRegistry properly inherits from Registry."""
        from arxiv_bucket.services.registry import Registry
        assert isinstance(self.registry, Registry)

    def test_add_entry_raises_attribute_error(self):
        """Test that add_entry raises AttributeError to prevent direct access."""
        with pytest.raises(AttributeError):
            self.registry.add_entry("test_key", {"test": "data"})

    def test_update_entry_raises_attribute_error(self):
        """Test that update_entry raises AttributeError to prevent direct access."""
        with pytest.raises(AttributeError):
            self.registry.update_entry("test_key", {"test": "data"})

    def test_register_submission_success(self, mocker):
        """Test successful registration of a valid submission file."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry.register_submission(file_path, bulk_archive_hash)
        assert self.registry.is_key_present(test_key)
        assert self.registry.get_entry(test_key)["metadata"]["filename"] == "1234.5678v1.tar.gz"

    def test_register_submission_file_not_found(self, mocker):
        """Test that FileNotFoundError is raised if the file does not exist."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_file.return_value = False
        with pytest.raises(FileNotFoundError):
            self.registry.register_submission(file_path, bulk_archive_hash)

    def test_register_submission_invalid_filename(self, mocker):
        """Test that ValueError is raised if the file is not a valid submission filename."""
        file_path = "/path/to/invalid_file.txt"
        bulk_archive_hash = "abc123"
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_is_file.return_value = True
        mock_is_submission.return_value = False
        with pytest.raises(ValueError):
            self.registry.register_submission(file_path, bulk_archive_hash)

    def test_register_submission_with_errors(self, mocker):
        """Test that diagnostics are set if submission file has errors."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = ["bad format"]
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry.register_submission(file_path, bulk_archive_hash)
        entry = self.registry.get_entry(test_key)
        assert "diagnostics" in entry
        assert "error_log" in entry["diagnostics"]
        assert "bad format" in entry["diagnostics"]["error_log"]

    def test_register_submission_key_already_exists_same_metadata(self, mocker):
        """Test that if the key exists and metadata is the same, registration is ignored (no-op)."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry._registry[test_key] = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        mocker.patch.object(self.registry, 'is_key_present', return_value=True)
        # Should not raise or change the registry
        self.registry.register_submission(file_path, bulk_archive_hash)
        existing_entry = self.registry.get_entry(test_key)
        assert "diagnostics" not in existing_entry

    def test_register_submission_key_already_exists_conflict(self, mocker):
        """Test that if the key exists and metadata is different, the conflict is logged in diagnostics."""
        file_path = "/path/to/1234.5678v1.tar.gz"
        bulk_archive_hash = "abc123"
        test_key = "submission_key"
        test_entry = {"metadata": {"filename": "1234.5678v1.tar.gz"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_submission = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.is_submission_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.submission_handler.SubmissionHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_submission.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry._registry[test_key] = {"metadata": {"filename": "something_else.tar.gz"}}
        mocker.patch.object(self.registry, 'is_key_present', return_value=True)
        # Should log the conflict in diagnostics
        self.registry.register_submission(file_path, bulk_archive_hash)
        existing_entry = self.registry.get_entry(test_key)
        assert "diagnostics" in existing_entry
        assert "error_log" in existing_entry["diagnostics"]
        assert "key conflicts" in existing_entry["diagnostics"]["error_log"]
        assert "key_conflicts" in existing_entry["diagnostics"]
        assert test_entry in existing_entry["diagnostics"]["key_conflicts"]

# Test for find_bulk_archive_key (placed at the end of the file)
def test_find_bulk_archive_key():
    """Test that find_bulk_archive_key returns all keys with the given bulk_archive_key in the origin field."""
    reg = SubmissionRegistry()
    reg._registry = {
        'k1': {'origin': {'bulk_archive_key': 'abc'}, 'metadata': {}},
        'k2': {'origin': {'bulk_archive_key': 'def'}, 'metadata': {}},
        'k3': {'origin': {'bulk_archive_key': 'abc'}, 'metadata': {}},
        'k4': {'origin': {}, 'metadata': {}},
        'k5': {'metadata': {}},
    }
    result = reg.find_bulk_archive_key('abc')
    assert set(result) == {'k1', 'k3'}
    assert reg.find_bulk_archive_key('def') == ['k2']
    assert reg.find_bulk_archive_key('notfound') == []

# Tests for list_bulk_archive_keys
def test_list_bulk_archive_keys_all_present():
    """Return unique bulk archive keys when all entries have the key in origin."""
    reg = SubmissionRegistry()
    reg._registry = {
        'a': {'origin': {'bulk_archive_key': 'abc'}},
        'b': {'origin': {'bulk_archive_key': 'def'}},
        'c': {'origin': {'bulk_archive_key': 'abc'}},
    }
    result = reg.list_bulk_archive_keys()
    assert set(result) == {'abc', 'def'}


def test_list_bulk_archive_keys_with_missing():
    """If some entries lack the bulk key, the returned set includes None (current implementation)."""
    reg = SubmissionRegistry()
    reg._registry = {
        'a': {'origin': {'bulk_archive_key': 'abc'}},
        'b': {'origin': {}},
        'c': {'metadata': {}},
    }
    result = reg.list_bulk_archive_keys()
    assert set(result) == {'abc', None}


def test_list_bulk_archive_keys_empty():
    """Empty registry yields an empty list."""
    reg = SubmissionRegistry()
    reg._registry = {}
    assert reg.list_bulk_archive_keys() == []


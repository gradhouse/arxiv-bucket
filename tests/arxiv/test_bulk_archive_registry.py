# File: test_bulk_archive_registry.py
# Description: Unit tests for the BulkArchiveRegistry class.
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
from arxiv_bucket.arxiv.bulk_archive_registry import BulkArchiveRegistry


class TestBulkArchiveRegistry:
    """Test cases for the BulkArchiveRegistry class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.registry = BulkArchiveRegistry()

    def test_inheritance(self):
        """Test that BulkArchiveRegistry properly inherits from Registry."""
        from arxiv_bucket.services.registry import Registry
        assert isinstance(self.registry, Registry)

    def test_add_entry_raises_attribute_error(self):
        """Test that add_entry raises AttributeError to prevent direct access."""
        with pytest.raises(AttributeError) as exc_info:
            self.registry.add_entry("test_key", {"test": "data"})
        
        assert "Direct access to base class 'add_entry' is not allowed" in str(exc_info.value)
        assert "Use 'register_bulk_archive' instead" in str(exc_info.value)

    def test_update_entry_raises_attribute_error(self):
        """Test that update_entry raises AttributeError to prevent direct access."""
        with pytest.raises(AttributeError) as exc_info:
            self.registry.update_entry("test_key", {"test": "data"})
        
        assert "Direct access to base class 'update_entry' is not allowed" in str(exc_info.value)
        assert "Use 'register_bulk_archive' instead" in str(exc_info.value)

    def test_find_bulk_archive_filename_found(self, mocker):
        """Test finding a bulk archive filename that exists in the registry."""
        mock_get_basename = mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename')
        mock_get_basename.return_value = "arXiv_src_9902_005.tar"
        test_key = "test_key"
        test_entry = {
            'metadata': {
                'filename': 'arXiv_src_9902_005.tar'
            }
        }
        self.registry._registry[test_key] = test_entry
        result = self.registry.find_bulk_archive_filename("/path/to/arXiv_src_9902_005.tar")
        assert result == test_key
        mock_get_basename.assert_called_once_with("/path/to/arXiv_src_9902_005.tar")

    def test_find_bulk_archive_filename_not_found(self, mocker):
        """Test finding a bulk archive filename that doesn't exist in the registry."""
        mock_get_basename = mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename')
        mock_get_basename.return_value = "nonexistent_file.tar"
        result = self.registry.find_bulk_archive_filename("/path/to/nonexistent_file.tar")
        assert result is None
        mock_get_basename.assert_called_once_with("/path/to/nonexistent_file.tar")

    def test_find_bulk_archive_filename_multiple_matches_returns_first(self, mocker):
        """Test that when multiple entries match, the first one is returned."""
        mock_get_basename = mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename')
        mock_get_basename.return_value = "arXiv_src_9902_005.tar"
        test_key1 = "test_key1"
        test_key2 = "test_key2"
        test_entry = {
            'metadata': {
                'filename': 'arXiv_src_9902_005.tar'
            }
        }
        self.registry._registry[test_key1] = test_entry
        self.registry._registry[test_key2] = test_entry
        result = self.registry.find_bulk_archive_filename("/path/to/arXiv_src_9902_005.tar")
        assert result in [test_key1, test_key2]

    def test_register_bulk_archive_success(self, mocker):
        """Test successful registration of a bulk archive."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        test_key = "test_key"
        test_entry = {"metadata": {"filename": "arXiv_src_9902_005.tar"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        mocker.patch.object(self.registry, 'find_bulk_archive_filename', return_value=None)
        mocker.patch.object(self.registry, 'is_key_present', return_value=False)
        self.registry.register_bulk_archive(file_path)
        mock_is_file.assert_called_once_with(file_path)
        mock_is_bulk_archive.assert_called_once_with(file_path)
        mock_generate_entry.assert_called_once_with(file_path)
        assert self.registry._registry[test_key] == test_entry

    def test_register_bulk_archive_file_not_found(self, mocker):
        """Test registration fails when file does not exist."""
        file_path = "/path/to/nonexistent.tar"
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_file.return_value = False
        with pytest.raises(FileNotFoundError) as exc_info:
            self.registry.register_bulk_archive(file_path)
        assert f"File '{file_path}' not found" in str(exc_info.value)
        mock_is_file.assert_called_once_with(file_path)

    def test_register_bulk_archive_invalid_filename(self, mocker):
        """Test registration fails when filename is not a valid bulk archive filename."""
        file_path = "/path/to/invalid_file.txt"
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = False
        with pytest.raises(ValueError) as exc_info:
            self.registry.register_bulk_archive(file_path)
        assert f"File '{file_path}' is not a valid bulk archive filename" in str(exc_info.value)
        mock_is_file.assert_called_once_with(file_path)
        mock_is_bulk_archive.assert_called_once_with(file_path)

    def test_register_bulk_archive_already_registered_by_filename(self, mocker):
        """Test registration fails when file is already registered."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mocker.patch.object(self.registry, 'find_bulk_archive_filename', return_value="existing_key")
        with pytest.raises(ValueError) as exc_info:
            self.registry.register_bulk_archive(file_path)
        assert f"File '{file_path}' is already registered" in str(exc_info.value)

    def test_register_bulk_archive_key_already_exists(self, mocker):
        pass

    def test_register_bulk_archive_with_errors(self, mocker):
        """Test registration fails when bulk archive has errors."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        test_key = "test_key"
        test_entry = {"metadata": {"filename": "arXiv_src_9902_005.tar"}}
        test_errors = ["Error 1", "Error 2"]
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        mocker.patch.object(self.registry, 'find_bulk_archive_filename', return_value=None)
        mocker.patch.object(self.registry, 'is_key_present', return_value=False)
        with pytest.raises(ValueError) as exc_info:
            self.registry.register_bulk_archive(file_path)
        assert f"Bulk archive '{file_path}' has errors: {test_errors}" in str(exc_info.value)

    def test_find_bulk_archive_filename_empty_registry(self, mocker):
        """Test finding a filename in an empty registry returns None."""
        mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename', return_value="test.tar")
        result = self.registry.find_bulk_archive_filename("/path/to/test.tar")
        assert result is None

    def test_find_bulk_archive_filename_entry_without_metadata(self, mocker):
        """Test finding a filename when registry entry doesn't have expected metadata structure."""
        mock_get_basename = mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename')
        mock_get_basename.return_value = "test.tar"
        test_key = "test_key"
        self.registry._registry[test_key] = {"invalid": "structure"}
        result = self.registry.find_bulk_archive_filename("/path/to/test.tar")
        assert result is None

    def test_registry_starts_empty(self):
        """Test that a new registry instance starts with an empty registry."""
        assert len(self.registry._registry) == 0

    def test_inherited_methods_accessible(self):
        """Test that inherited methods from Registry base class are accessible."""
        # These methods should be accessible from the base class
        assert hasattr(self.registry, 'is_key_present')
        assert hasattr(self.registry, 'get_entry')
        assert hasattr(self.registry, 'clear')
        
        # Test that base class methods work
        assert self.registry.is_key_present("nonexistent") is False

    def test_register_bulk_archive_key_already_exists_same_metadata(self, mocker):
        """If the key exists and metadata is the same, do nothing (ignore)."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        test_key = "existing_key"
        test_entry = {"metadata": {"filename": "arXiv_src_9902_005.tar"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry._registry[test_key] = {"metadata": {"filename": "arXiv_src_9902_005.tar"}}
        mocker.patch.object(self.registry, 'find_bulk_archive_filename', return_value=None)
        self.registry.register_bulk_archive(file_path)
        existing_entry = self.registry._registry[test_key]
        assert "diagnostics" not in existing_entry

    def test_register_bulk_archive_key_already_exists_conflict(self, mocker):
        """If the key exists and metadata is different, should raise KeyError."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        test_key = "existing_key"
        test_entry = {"metadata": {"filename": "arXiv_src_9902_005.tar"}}
        test_errors = []
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.generate_registry_entry')
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry._registry[test_key] = {"metadata": {"filename": "something_else.tar"}}
        mocker.patch.object(self.registry, 'find_bulk_archive_filename', return_value=None)
        with pytest.raises(KeyError):
            self.registry.register_bulk_archive(file_path)


class TestBulkArchiveRegistryIntegration:
    """Integration tests for BulkArchiveRegistry with real method calls."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.registry = BulkArchiveRegistry()

    def test_register_and_find_integration(self, mocker):
        """Test the integration between register_bulk_archive and find_bulk_archive_filename."""
        file_path = "/path/to/arXiv_src_9902_005.tar"
        basename = "arXiv_src_9902_005.tar"
        test_key = "generated_key"
        test_entry = {"metadata": {"filename": basename}}
        test_errors = []
        mock_get_basename = mocker.patch('arxiv_bucket.file.file_name.FileName.get_file_basename')
        mock_is_file = mocker.patch('arxiv_bucket.file.file_system.FileSystem.is_file')
        mock_is_bulk_archive = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.is_bulk_archive_filename')
        mock_generate_entry = mocker.patch('arxiv_bucket.arxiv.bulk_archive_handler.BulkArchiveHandler.generate_registry_entry')
        mock_get_basename.return_value = basename
        mock_is_file.return_value = True
        mock_is_bulk_archive.return_value = True
        mock_generate_entry.return_value = (test_key, test_entry, test_errors)
        self.registry.register_bulk_archive(file_path)
        found_key = self.registry.find_bulk_archive_filename(file_path)
        assert found_key == test_key
        assert self.registry.is_key_present(test_key)
        assert self.registry.get_entry(test_key) == test_entry

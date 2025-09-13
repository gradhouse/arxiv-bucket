# File: bulk_archive_registry.py
# Description: bulk archive registry
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

from arxiv_bucket.file.file_name import FileName
from arxiv_bucket.file.file_system import FileSystem
from arxiv_bucket.services.registry import Registry

from .bulk_archive_handler import BulkArchiveHandler

class BulkArchiveRegistry(Registry):
    """
    Registry for bulk archives.
    """

    def add_entry(self, hash_key: str, entry: dict) -> None:
        """
        Prevent direct access to the base class 'add_entry' method.

        This method is overridden to raise an AttributeError, ensuring that
        entries are added to the registry only through the 'register_bulk_archive' method.

        :param hash_key: str, the unique key for the registry entry (not used here).
        :param entry: dict, the registry entry data (not used here).

        :raises AttributeError: Always raised to enforce the use of 'register_bulk_archive'.
        """
        raise AttributeError("Direct access to base class 'add_entry' is not allowed. Use 'register_bulk_archive' instead.")

    def update_entry(self, hash_key: str, entry: dict) -> None:
        """
        Prevent direct access to the base class 'update_entry' method.

        This method is overridden to raise an AttributeError, ensuring that
        entries are updated in the registry only through the 'register_bulk_archive' method.

        :param hash_key: str, the unique key for the registry entry (not used here).
        :param entry: dict, the registry entry data (not used here).

        :raises AttributeError: Always raised to enforce the use of 'register_bulk_archive'.
        """
        raise AttributeError("Direct access to base class 'update_entry' is not allowed. Use 'register_bulk_archive' instead.")

    def find_bulk_archive_filename(self, file_path: str) -> str | None:
        """
        Find the key corresponding filename of a registered bulk archive.

        :param file_path: str, the name of the bulk archive filename
        :return: str | None, the registry key of the bulk archive filename if present in the registry, or None if not found
        """

        basename = FileName.get_file_basename(file_path)
        registry_keys = [
            key for key, entry in self._registry.items() 
            if entry.get('metadata', {}).get('filename') == basename
        ]

        registry_key = registry_keys[0] if registry_keys else None
        return registry_key

    def register_bulk_archive(self, file_path: str) -> None:
        """
        Register a bulk archive file.

        :param file_path: str, the name of the bulk archive filename

        :raises FileNotFoundError: if the file does not exist
        :raises ValueError: if the file is not a valid bulk archive filename
        :raises ValueError: if the bulk archive has errors
        :raises KeyError: if the bulk archive is already registered
        """

        if not FileSystem.is_file(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")
        
        if not BulkArchiveHandler.is_bulk_archive_filename(file_path):
            raise ValueError(f"File '{file_path}' is not a valid bulk archive filename.")
        
        if self.find_bulk_archive_filename(file_path) is not None:
            raise ValueError(f"File '{file_path}' is already registered.")

        registry_key, registry_entry, bulk_archive_errors = BulkArchiveHandler.generate_registry_entry(file_path)
                        
        if len(bulk_archive_errors) > 0:
            raise ValueError(f"Bulk archive '{file_path}' has errors: {bulk_archive_errors}")
        
        if self.is_key_present(registry_key):
            if self.get_entry(registry_key) != registry_entry:
                raise KeyError(f"Bulk archive with key '{registry_key}' is already registered.")
            else:
                return  # No action needed if the entry is identical

        super().add_entry(registry_key, registry_entry)

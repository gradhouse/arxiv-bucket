# File: submission_registry.py
# Description: submission registry
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

from .submission_handler import SubmissionHandler

class SubmissionRegistry(Registry):
    """
    Registry for submissions.
    """

    def add_entry(self, hash_key: str, entry: dict) -> None:
        """
        Prevent direct access to the base class 'add_entry' method.

        This method is overridden to raise an AttributeError, ensuring that
        entries are added to the registry only through the 'register_submission' method.

        :param hash_key: str, the unique key for the registry entry (not used here).
        :param entry: dict, the registry entry data (not used here).

        :raises AttributeError: Always raised to enforce the use of 'register_submission'.
        """
        raise AttributeError("Direct access to base class 'add_entry' is not allowed. Use 'register_submission' instead.")

    def update_entry(self, hash_key: str, entry: dict) -> None:
        """
        Prevent direct access to the base class 'update_entry' method.

        This method is overridden to raise an AttributeError, ensuring that
        entries are updated in the registry only through the 'register_submission' method.

        :param hash_key: str, the unique key for the registry entry (not used here).
        :param entry: dict, the registry entry data (not used here).

        :raises AttributeError: Always raised to enforce the use of 'register_submission'.
        """
        raise AttributeError("Direct access to base class 'update_entry' is not allowed. Use 'register_submission' instead.")

    def register_submission(self, file_path: str, bulk_archive_key: str) -> None:
        """
        Register a submission file.
        If the submission file has errors, they will be logged in the diagnostics error log registry entry.

        :param file_path: str, the name of the submission filename
        :param bulk_archive_key: str, the key of the bulk archive entry that contained the submission, this is currently the SHA256 hash

        :raises FileNotFoundError: if the file does not exist
        :raises ValueError: if the file is not a valid submission filename
        """

        if not FileSystem.is_file(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")

        if not SubmissionHandler.is_submission_filename(file_path):
            raise ValueError(f"File '{file_path}' is not a valid submission filename.")

        registry_key, registry_entry, submission_errors = SubmissionHandler.generate_registry_entry(file_path, bulk_archive_key=bulk_archive_key)
        
        if len(submission_errors) > 0:
            registry_entry['diagnostics'] = {'error_log': submission_errors}

        if self.is_key_present(registry_key):
            # duplicate key detected
            existing_entry = self.get_entry(registry_key)
            if existing_entry['metadata'] != registry_entry['metadata']:   # different submissions same key
                diagnostics = existing_entry.setdefault('diagnostics', {})
                error_log = diagnostics.setdefault('error_log', [])
                if 'key conflicts' not in error_log:
                    error_log.append('key conflicts')
                    diagnostics['key_conflicts'] = []                
                diagnostics['key_conflicts'].append(registry_entry)
            return

        super().add_entry(registry_key, registry_entry)

    def find_submission_filename(self, file_path: str) -> list[str]:
        """
        Find the keys corresponding to the filename of a registered submission.

        :param file_path: str, the name of the submission filename
        :return: list[str], the registry keys of the submission filename if present in the registry, or an empty list if not found
        """

        basename = FileName.get_file_basename(file_path)
        registry_keys = [
            key for key, entry in self._registry.items() 
            if entry.get('metadata', {}).get('filename') == basename
        ]

        return registry_keys
    
    def is_entry_valid(self, key: str) -> bool:
        """
        Check if a registry entry is valid.

        A valid entry has no diagnostics errors.

        :param key: str, the registry key to check
        :return: bool, True if the entry is valid, False otherwise

        :raises KeyError: if the key is not found in the registry
        """

        if not self.is_key_present(key):
            raise KeyError(f"Key '{key}' not found in registry.")

        entry = self.get_entry(key)
        diagnostics = entry.get('diagnostics', {})
        error_log = diagnostics.get('error_log', [])

        return len(error_log) == 0

    def list_invalid_entries(self) -> list[str]:
        """
        List all invalid registry entries.

        :return: list[str], the keys of all invalid entries
        """

        return [
            key for key in self._registry.keys()
            if not self.is_entry_valid(key)
        ]

    def find_bulk_archive_key(self, bulk_archive_key: str) -> list[str]:
        """
        Find all registry keys associated with a given bulk archive key (now searched in the 'origin' field).

        :param bulk_archive_key: str, the bulk archive key to search for
        :return: list[str], the registry keys associated with the bulk archive key
        """

        return [
            key for key, entry in self._registry.items()
            if entry.get('origin', {}).get('bulk_archive_key') == bulk_archive_key
        ]

    def list_bulk_archive_keys(self) -> list[str]:
        """
        List all unique bulk archive keys present in the registry.

        :return: list[str], the unique bulk archive keys
        """

        return list({
            entry.get('origin', {}).get('bulk_archive_key')
            for entry in self._registry.values()
        })
    
# File: bulk_archive_handler.py
# Description: arXiv bulk archive handler methods
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

from __future__ import annotations

import re
from typing import Optional, Tuple, cast

from arxiv_bucket.file.file_handler import FileHandler
from arxiv_bucket.file.file_name import FileName
from arxiv_bucket.file.file_system import FileSystem
from arxiv_bucket.file.file_type import FileType
from arxiv_bucket.file.handler.archive_handler import ArchiveHandler

from arxiv_bucket.services.hash_service import HashType

from .submission_handler import SubmissionHandler


class BulkArchiveHandler:
    """
    Bulk archive handler methods
    """

    @staticmethod
    def parse_bulk_archive_filename(filename: str) -> Optional[Tuple[str, str, str]]:
        """
        Extract the year, month, and sequence number from a bulk archive filename.

        Bulk archive filenames follow the format: arXiv_src_{yymm}_{seq_num}.tar
        - yy: last two digits of the year (e.g., '99' for 1999)
        - mm: two-digit month number (e.g., '02' for February)
        - seq_num: three-digit sequence number within the month (e.g., '005' for the fifth file)        

        For example: arXiv_src_9902_005.tar -> ('99', '02', '005')

        :param filename: str, the bulk archive filename
        :return: tuple[str, str, str] or None, (yy, mm, seq_num) if the filename matches the pattern.
            If the filename does not match the pattern then None is returned.
        """

        basename = FileName.get_file_basename(filename)
        match = re.match(r'^arXiv_src_(\d{2})(\d{2})_(\d{3})\.tar$', basename)
        if match:
            result = cast(Tuple[str, str, str], match.groups())
        else:
            result = None
        return result

    @staticmethod
    def is_bulk_archive_filename(filename: str) -> bool:
        """
        Determine if the given filename is a valid arXiv bulk archive filename.

        A valid bulk archive filename must:
        - Match the pattern 'arXiv_src_{yymm}_{seq_num}.tar'
        - Have a two-digit month (mm) in the range '01' to '12'

        :param filename: str, the filename to check (can include a path)
        :return: bool, True if the filename matches the bulk archive pattern and has a valid month, False otherwise
        """
        result = False
        parts = BulkArchiveHandler.parse_bulk_archive_filename(filename)
        if parts:
            _, mm, _ = parts
            if mm.isdigit() and 1 <= int(mm) <= 12:
                result = True
        return result
    
    @staticmethod
    def generate_uri_for_bulk_archive_filename(filename: str) -> str:
        """
        Generate the bulk archive Uniform Resource Identifier (URI) for the given arXiv bulk archive filename.
        The filename should follow the bulk archive file naming scheme.

        The base name of the filename is determined and the URI generated. For example
            local_directory/arXiv_src_9902_005.tar generates the URI 's3://arxiv/src/arXiv_src_9902_005.tar'

        :param filename: filename of the bulk archive file
        :return: str, URI of the bulk archive file

        :raises ValueError: if the filename does not follow the bulk archive file naming scheme
        """

        base_uri = 's3://arxiv/src/'
        if not BulkArchiveHandler.is_bulk_archive_filename(filename):
            raise ValueError(f"Filename {filename} does not match arXiv bulk archive naming scheme")

        base_filename = FileName.get_file_basename(filename)
        return f"{base_uri}{base_filename}"

    @staticmethod
    def check_bulk_archive(file_path: str) -> list[str]:
        """
        Validate an arXiv bulk archive file and return a list of error messages describing any issues found.

        This method performs a series of checks to ensure the file is a valid arXiv bulk archive:
          1. Checks that the filename matches the expected arXiv bulk archive naming pattern.
          2. Checks that the file exists at the specified path.
          3. Checks that the file extension and format are both recognized as a tar archive.
          4. Checks that the archive can be safely extracted (e.g., no filename collisions, traversal, etc.).
          5. Checks that all entries inside the archive match the expected arXiv submission filename pattern.

        The method stops further checks if a critical error is found at any stage (e.g., invalid filename or file not found).

        :param file_path: str, path to the bulk archive file to validate.
        :returns: list[str], a list of error messages.
            If the list is empty, the bulk archive file is considered valid and extraction is possible.
        """

        error_list = []

        # Check filename pattern
        if not BulkArchiveHandler.is_bulk_archive_filename(file_path):
            error_list.append(f'Filename {file_path} does not match bulk archive pattern')

        # Check file existence only if pattern is valid
        if not error_list:
            if not FileSystem.is_file(file_path):
                error_list.append(f'File {file_path} not found')

        # Check file type only if previous checks passed
        if not error_list:
            if FileType.FILE_TYPE_ARCHIVE_TAR not in FileHandler.get_file_type_from_extension(file_path):
                error_list.append('File extension is not tar')
            elif FileType.FILE_TYPE_ARCHIVE_TAR != FileHandler.get_file_type_from_format(file_path):
                error_list.append('File format is not tar')

        # check that the archive could be in principle extracted
        if not error_list:
            default_extract_path = '/'
            archive_errors = ArchiveHandler.check_extract_possible(file_path, default_extract_path)
            error_list.extend(archive_errors)

        if not error_list:
            archive_contents = ArchiveHandler.list_contents(file_path)
            invalid_entries = [entry for entry in archive_contents if not SubmissionHandler.is_submission_filename(entry)]
            if invalid_entries:
                error_list.append(f"Archive entries do not match submission filename pattern: {', '.join(invalid_entries)}")

        return error_list

    @staticmethod
    def is_bulk_archive_valid(file_path: str) -> bool:
        """
        Check if the given file is a valid arXiv bulk archive.
        See check_bulk_archive() for the list of checks.

        :param file_path: str, path to the bulk archive file to validate.
        :return: bool, True if the file is a valid bulk archive, False otherwise.
        """

        return len(BulkArchiveHandler.check_bulk_archive(file_path)) == 0
    
    @staticmethod
    def generate_registry_entry(file_path: str) -> tuple[str, dict, list[str]]:
        """
        Generate the key-value pair for an arXiv bulk archive registry entry.

        :param file_path: str, file path to the bulk archive entry
        :return: tuple[str, dict, list[str]], key, value pair for the registry entry and list of bulk archive errors
            The key is the SHA256 hash for the bulk archive
            The value is a dictionary with the fields 'metadata' and 'origin'.
            If the bulk archive has an error then the corresponding errors will be added to a list

        :raises FileNotFoundError: if the bulk archive entry does not exist
        """

        hash_types = [HashType.HASH_TYPE_MD5, HashType.HASH_TYPE_SHA256]

        if not FileSystem.is_file(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")

        bulk_archive_errors = BulkArchiveHandler.check_bulk_archive(file_path)
        uri = BulkArchiveHandler.generate_uri_for_bulk_archive_filename(file_path)
        metadata = FileHandler.get_metadata(file_path, hash_types=hash_types)

        registry_key = metadata['hash']['SHA256']

        registry_entry = {
            'metadata': metadata,
            'origin': {
                'uri': uri
            }
        }

        return registry_key, registry_entry, bulk_archive_errors

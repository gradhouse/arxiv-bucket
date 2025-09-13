# File: submission_handler.py
# Description: Submission methods.
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

import os
import re

from arxiv_bucket.file.file_handler import FileHandler
from arxiv_bucket.file.file_name import FileName
from arxiv_bucket.file.file_system import FileSystem
from arxiv_bucket.file.file_type import FileType

from arxiv_bucket.file.handler.archive_handler import ArchiveHandler

from arxiv_bucket.services.hash_service import HashType

from .submission_type import SubmissionType


class SubmissionHandler:
    """
    Submission handler methods
    """

    @staticmethod
    def get_submission_type_using_extension(submission_file_list: list[str]) -> SubmissionType:
        """
        Get the submission type category from the contents extensions.

        This method inspects the list of files in the submission and determines the overall submission type
        based on the detected file types. The classification is as follows:

        - If all files are PostScript (.ps), returns SUBMISSION_TYPE_POSTSCRIPT.
        - If all files are PDF (.pdf), returns SUBMISSION_TYPE_PDF.
        - If at least one TeX/LaTeX main file is present and all other files are recognized as TeX/LaTeX
          supporting files, returns SUBMISSION_TYPE_TEX.
        - If any file is unrecognized or not associated with the detected main type, returns SUBMISSION_TYPE_UNKNOWN.

        :param submission_file_list: str, list of files in the submission
        :return: SubmissionType, matching submission type category
        """

        tex_types = {FileType.FILE_TYPE_TEX_TEX, FileType.FILE_TYPE_TEX_LATEX_209_MAIN,
                     FileType.FILE_TYPE_TEX_LATEX_2E_MAIN}
        tex_supporting_types = {FileType.FILE_TYPE_TEX_LOG, FileType.FILE_TYPE_TEX_FIG, FileType.FILE_TYPE_IMAGE_GIF,
                                FileType.FILE_TYPE_IMAGE_PNG, FileType.FILE_TYPE_IMAGE_JPG, FileType.FILE_TYPE_TEX_BIB,
                                FileType.FILE_TYPE_TEX_CLO, FileType.FILE_TYPE_TEX_BST, FileType.FILE_TYPE_TEX_TOC,
                                FileType.FILE_TYPE_TEX_CLS, FileType.FILE_TYPE_TEX_BBL, FileType.FILE_TYPE_POSTSCRIPT_EPSF,
                                FileType.FILE_TYPE_TEX_PSTEX_T, FileType.FILE_TYPE_TEX_PSTEX, FileType.FILE_TYPE_TEX_STY,
                                FileType.FILE_TYPE_TEX_LATEX_209_MAIN, FileType.FILE_TYPE_TEX_LATEX_2E_MAIN,
                                FileType.FILE_TYPE_TEX_TEX, FileType.FILE_TYPE_PDF, FileType.FILE_TYPE_POSTSCRIPT_PS,
                                FileType.FILE_TYPE_POSTSCRIPT_EPSI, FileType.FILE_TYPE_POSTSCRIPT_EPS}

        file_types = []
        for filename in submission_file_list:
            current_file_type_list = FileHandler.get_file_type_from_extension(filename)
            if len(current_file_type_list) == 0:
                file_types.append(FileType.FILE_TYPE_UNKNOWN)
            else:
                file_types.extend(current_file_type_list)
        file_types = set(file_types)

        if {FileType.FILE_TYPE_POSTSCRIPT_PS} == file_types:
            # postscript submission type
            submission_type = SubmissionType.SUBMISSION_TYPE_POSTSCRIPT
        elif {FileType.FILE_TYPE_PDF} == file_types:
            # pdf submission type
            submission_type = SubmissionType.SUBMISSION_TYPE_PDF
        elif len(tex_types & file_types) > 0:
            # potential TeX or LaTeX submission type
            residual = file_types - tex_supporting_types
            if len(residual) == 0:
                # all file types are TeX / LaTeX associated
                submission_type = SubmissionType.SUBMISSION_TYPE_TEX
            else:
                # at least one file type is not TeX or LaTeX associated
                submission_type = SubmissionType.SUBMISSION_TYPE_UNKNOWN
        else:
            # file types are not associated with a known submission type
            submission_type = SubmissionType.SUBMISSION_TYPE_UNKNOWN

        return submission_type

    @staticmethod
    def generate_registry_entry(file_path: str, bulk_archive_key: str) -> tuple[str, dict, list[str]]:
        """
        Generate the key-value pair for an arXiv submission registry entry.

        :param file_path: str, file path to the submission entry
        :param bulk_archive_key: str, key of the bulk archive entry that contained the submission, this is currently the SHA256 hash
        :return: tuple[str, dict, list[str]], key, value pair for the registry entry and list of submission errors
            The key is the SHA256 hash for the submission
            The value is a dictionary with the fields 'metadata' and 'origin'.
            If the submission has an error then the corresponding errors will be added to a list

        :raises FileNotFoundError: if the submission entry does not exist
        """

        hash_types = [HashType.HASH_TYPE_MD5, HashType.HASH_TYPE_SHA256]

        if not FileSystem.is_file(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")

        submission_errors = SubmissionHandler.check_submission(file_path)
        url = SubmissionHandler.generate_url_for_submission_filename(file_path)
        metadata = FileHandler.get_metadata(file_path, hash_types=hash_types)

        registry_key = metadata['hash']['SHA256']

        if len(submission_errors) > 0:
            submission_type = SubmissionType.SUBMISSION_TYPE_UNKNOWN
        else:
            if metadata['file_type'] in [FileType.FILE_TYPE_PDF.value]:
                submission_type = SubmissionType.SUBMISSION_TYPE_PDF
            else:
                if metadata['file_type'] in [FileType.FILE_TYPE_ARCHIVE_GZ.value, FileType.FILE_TYPE_ARCHIVE_TGZ.value]:
                    archive_contents = ArchiveHandler.list_contents(file_path)
                    submission_type = SubmissionHandler.get_submission_type_using_extension(archive_contents)
                else:
                    submission_type = SubmissionType.SUBMISSION_TYPE_UNKNOWN

        metadata['submission_type_by_extension'] = submission_type.value

        if len(submission_errors) == 0 and submission_type == SubmissionType.SUBMISSION_TYPE_UNKNOWN:
            submission_errors.append('Unknown submission type')

        registry_entry = {
            'metadata': metadata,
            'origin': {
                'url': url,
                'bulk_archive_key': bulk_archive_key
            }
        }

        return registry_key, registry_entry, submission_errors

    @staticmethod
    def parse_old_style_submission_filename(filename: str) -> tuple[str, str, str, str] | None:
        """
        Parse an older-style arXiv submission filename (pre-2008) and extract its components.

        Older arXiv submission filenames follow the pattern:
            {category}{yy}{mm}{number}.{ext}
        where:
            - category: subject area (e.g., 'cond-mat')
            - yy: last two digits of the year (e.g., '96' for 1996)
            - mm: two-digit month (e.g., '02' for February)
            - number: submission number within the month (e.g., '101')
            - ext: file extension, either '.gz' or '.pdf'

        Example:
            'cond-mat9602101.gz' → ('cond-mat', '96', '02', '101')
            (This corresponds to cond-mat/9602101 on arXiv, submitted in February 1996.)

        :param filename: str, the submission filename (may include a path)
        :return: tuple (category, yy, mm, number) if the filename matches the pattern, else None
        """

        basename = FileName.get_file_basename(filename)
        basename_no_ext, ext = os.path.splitext(basename)
        result = None
        if ext in {'.gz', '.pdf'}:
            match = re.match(r'^([a-z\-]+)(\d{2})(\d{2})(\d{3})$', basename_no_ext)
            if match:
                category, yy, mm, number = match.groups()
                result = (category, yy, mm, number)
        return result

    @staticmethod
    def parse_current_style_submission_filename(filename: str) -> tuple[str, str, str] | None:
        """
        Parse a newer-style arXiv submission filename and extract its components.

        Newer arXiv submission filenames follow the pattern:
            {yymm}.{number}.{ext}
        where:
            - yy: last two digits of the year (e.g., '12' for 2012)
            - mm: two-digit month (e.g., '02' for February)
            - number: submission number within the month (e.g., '3054')
            - ext: file extension, either '.gz' or '.pdf'

        Example:
            '1202.3054.gz' → ('12', '02', '3054')
            (This corresponds to arXiv/1202.3054, submitted in February 2012.)

        :param filename: str, the submission filename (may include a path)
        :return: tuple (yy, mm, number) if the filename matches the pattern, else None
        """
        basename = FileName.get_file_basename(filename)
        basename_no_ext, ext = os.path.splitext(basename)
        result = None
        if ext in {'.gz', '.pdf'}:
            match = re.match(r'^(\d{2})(\d{2})\.(\d{4,5})$', basename_no_ext)
            if match:
                yy, mm, number = match.groups()
                result = (yy, mm, number)
        return result

    @staticmethod
    def is_submission_filename(filename: str) -> bool:
        """
        Determine if the provided file path corresponds to the arXiv submission filename scheme.

        The filename must match either the old or current style submission pattern,
        and the extracted month must be between '01' and '12' (inclusive).

        :param filename: str, the submission filename (may include a path)
        :return: bool, True if the filename matches a submission pattern and has a valid month, else False
        """
        result = False
        old = SubmissionHandler.parse_old_style_submission_filename(filename)
        if old is not None:
            _, _, mm, _ = old
            result = mm.isdigit() and 1 <= int(mm) <= 12
        else:
            current = SubmissionHandler.parse_current_style_submission_filename(filename)
            if current is not None:
                _, mm, _ = current
                result = mm.isdigit() and 1 <= int(mm) <= 12
        return result

    @staticmethod
    def generate_url_for_submission_filename(filename: str) -> str:
        """
        Generate the URL for the given arXiv submission filename.

        Submission filenames have one of the two patterns:
            1. Subject and number (older classification scheme, pre-2008):
               e.g., cond-mat9602101.gz → https://arxiv.org/abs/cond-mat/9602101
            2. Number only (newest classification scheme):
               e.g., 1202.3054.gz → https://arxiv.org/abs/1202.3054

        The method uses the appropriate parsing method to extract the relevant components and constructs
        the canonical arXiv URL for the submission.

        :param filename: str, file name (may include a path)
        :return: str, URL pointer to arXiv for the specified arXiv submission

        :raises ValueError: if the filename does not match a valid arXiv submission pattern.
        """
        base_url = 'https://arxiv.org/abs/'

        url = None
        old = SubmissionHandler.parse_old_style_submission_filename(filename)
        if old is not None:
            category, yy, mm, number = old
            url = f"{base_url}{category}/{yy}{mm}{number}"
        else:
            current = SubmissionHandler.parse_current_style_submission_filename(filename)
            if current is not None:
                yy, mm, number = current
                url = f"{base_url}{yy}{mm}.{number}"
        if url is None:
            raise ValueError(f"Invalid arXiv submission filename: {filename}")
        
        return url


    @staticmethod
    def check_submission(file_path: str) -> list[str]:
        """
        Validate an arXiv submission file and return a list of error messages describing any issues found.

        This method performs a series of checks to ensure the file is a valid arXiv submission:
          1. Checks that the filename matches the expected arXiv submission naming pattern.
          2. Checks that the file exists at the specified path.
          3. Checks that the file extension and format are among the allowed types (GZ, TGZ, or PDF).
          4. Checks that the file format matches the file extension.
          5. If the file is an archive (GZ or TGZ), checks that it can be safely extracted.

        The method stops further checks if a critical error is found at any stage (e.g., invalid filename or file not found).

        :param file_path: str, path to the submission file to validate.
        :return: list[str], a list of error messages. If the list is empty, the file is considered valid.
        """

        allowed_file_types = [FileType.FILE_TYPE_ARCHIVE_GZ, FileType.FILE_TYPE_ARCHIVE_TGZ, FileType.FILE_TYPE_PDF]
        allowed_archive_file_types = [FileType.FILE_TYPE_ARCHIVE_GZ, FileType.FILE_TYPE_ARCHIVE_TGZ]

        error_list = []

        # Check filename pattern
        if not SubmissionHandler.is_submission_filename(file_path):
            error_list.append(f'Filename {file_path} does not match submission pattern')

        # Check file existence only if pattern is valid
        if not error_list:
            if not FileSystem.is_file(file_path):
                error_list.append(f'File {file_path} not found')

        # Check file type only if previous checks passed
        if not error_list:
            file_types_by_extension = FileHandler.get_file_type_from_extension(file_path)
            file_type_by_format = FileHandler.get_file_type_from_format(file_path)

            if not any(file_type in allowed_file_types for file_type in file_types_by_extension):
                error_list.append('File extension type is not allowed')
            elif file_type_by_format not in allowed_file_types:
                error_list.append(f'File type {file_type_by_format.value} not allowed')
            elif file_type_by_format not in file_types_by_extension:
                error_list.append('File format does not match file extension')

            # check that the archive could be in principle extracted
            if not error_list and (file_type_by_format in allowed_archive_file_types):
                default_extract_path = '/'
                archive_errors = ArchiveHandler.check_extract_possible(file_path, default_extract_path)
                error_list.extend(archive_errors)

        return error_list

    @staticmethod
    def is_submission_valid(file_path: str) -> bool:
        """
        Check if the given file is a valid arXiv submission.
        See check_submission() for the list of checks.

        :param file_path: str, path to the submission file to validate.
        :return: bool, True if the file is a validsubmission, False otherwise.
        """

        return len(SubmissionHandler.check_submission(file_path)) == 0

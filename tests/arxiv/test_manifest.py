# File: test_manifest.py
# Description: Unit tests for the Manifest class.
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

import copy
import matplotlib.pyplot as plt
import os
import pytest
import tempfile
import warnings

from arxiv_bucket.arxiv.manifest import Manifest

# Sample valid xml_dict
valid_xml_dict = {
    'arXivSRC': {
        'file': [
            {
                'content_md5sum': 'cacbfede21d5dfef26f367ec99384546',
                'filename': 'src/arXiv_src_0001_001.tar',
                'first_item': 'astro-ph0001001',
                'last_item': 'quant-ph0001119',
                'md5sum': '949ae880fbaf4649a485a8d9e07f370b',
                'num_items': '2364',
                'seq_num': '1',
                'size': '225605507',
                'timestamp': '2010-12-23 00:13:59',
                'yymm': '0001'
            }
        ],
        'timestamp': '2010-12-23 00:00:00'
    }
}

def test_manifest_constructor():
    """
    Test the constructor of the Manifest class.
    Ensure that the manifest is initialized with default values.
    """
    manifest = Manifest()
    assert manifest._manifest == {
        'metadata': {},
        'contents': dict()
    }

def test_manifest_clear():
    """
    Test the clear method of the Manifest class.
    Ensure that the manifest is cleared and reset to default values.
    """
    manifest = Manifest()
    # Modify the manifest to simulate existing data

    manifest._manifest['metadata'] = {'key': 'value'}
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {
            'filename': 'src/arXiv_src_0001_001.tar',
            'size_bytes': 225605507,
            'timestamp_iso': '2010-12-23T05:13:59+00:00',
            'year': 2000,
            'month': 1,
            'sequence_number': 1,
            'n_submissions': 2364,
            'hash': {
                'MD5': '949ae880fbaf4649a485a8d9e07f370b',
                'MD5_contents': 'cacbfede21d5dfef26f367ec99384546'
            }
        }
    }

    # Call the clear method
    manifest.clear()

    # Assert that the manifest is reset to default values
    assert manifest._manifest == {
        'metadata': {},
        'contents': dict()
    }

def test_manifest_set_defaults():
    """
    Test the _set_defaults method of the Manifest class.
    Ensure that the manifest is set to default values.
    """
    manifest = Manifest()
    # Modify the manifest to simulate existing data
    manifest._manifest['metadata'] = {'key': 'value'}
    manifest._manifest['contents'] = {'test.txt': {'filename': 'test.txt'}}

    # Call the _set_defaults method
    manifest._set_defaults()

    # Assert that the manifest is reset to default values
    assert manifest._manifest == {
        'metadata': {},
        'contents': dict()
    }

def test_is_arxiv_keys_present_with_valid_data():
    """
    Test that the method returns True for a valid xml_dict.
    """
    assert Manifest._is_arxiv_keys_present(valid_xml_dict) is True

def test_is_arxiv_keys_present_with_missing_top_level_key():
    """
    Test that the method returns False when the top-level key is missing.
    """
    invalid_dict = {}
    assert Manifest._is_arxiv_keys_present(invalid_dict) is False

def test_is_arxiv_keys_present_with_missing_arxivsrc_keys():
    """
    Test that the method returns False when 'arXivSRC' keys are missing.
    """

    invalid_dict = copy.deepcopy(valid_xml_dict)
    del invalid_dict['arXivSRC']['file']

    assert Manifest._is_arxiv_keys_present(invalid_dict) is False

    invalid_dict = copy.deepcopy(valid_xml_dict)
    del invalid_dict['arXivSRC']['timestamp']

    assert Manifest._is_arxiv_keys_present(invalid_dict) is False


def test_is_arxiv_keys_valid_with_non_string_value_in_timestamp():
    """
    Test that the method returns False when a value in 'file' is not a string.
    """
    invalid_dict = copy.deepcopy(valid_xml_dict)
    invalid_dict['arXivSRC']['timestamp'] = 1234  # should be a string

    assert Manifest._is_arxiv_keys_present(invalid_dict) is False


def test_is_arxiv_keys_present_with_invalid_file_structure():
    """
    Test that the method returns False when 'file' entries have missing keys.
    """
    invalid_dict = {
        'arXivSRC': {
            'file': [
                {
                    'content_md5sum': 'cacbfede21d5dfef26f367ec99384546',
                    'filename': 'src/arXiv_src_0001_001.tar'
                    # Missing other required keys
                }
            ],
            'timestamp': '2010-12-23 00:00:00'
        }
    }
    assert Manifest._is_arxiv_keys_present(invalid_dict) is False

def test_is_arxiv_keys_present_with_invalid_types():
    """
    Test that the method returns False when data types are incorrect.
    """
    invalid_dict = {
        'arXivSRC': {
            'file': [
                {
                    'content_md5sum': 12345,  # Should be a string
                    'filename': 'src/arXiv_src_0001_001.tar',
                    'first_item': 'astro-ph0001001',
                    'last_item': 'quant-ph0001119',
                    'md5sum': '949ae880fbaf4649a485a8d9e07f370b',
                    'num_items': '2364',
                    'seq_num': '1',
                    'size': '225605507',
                    'timestamp': '2010-12-23 00:13:59',
                    'yymm': '0001'
                }
            ],
            'timestamp': '2010-12-23 00:00:00'
        }
    }
    assert Manifest._is_arxiv_keys_present(invalid_dict) is False

def test_convert_arxiv_timestamp_to_iso():
    """
    Test _convert_arxiv_timestamp_to_iso method for converting EST timestamp to ISO 8601 GMT.
    """
    # Input timestamp in EST
    input_timestamp = 'Mon Apr  7 04:58:03 2025'
    # Expected output in GMT
    expected_output = '2025-04-07T08:58:03+00:00'

    # Call the method
    result = Manifest._convert_arxiv_timestamp_to_iso(input_timestamp)

    # Assert the result matches the expected output
    assert result == expected_output


def test_convert_arxiv_file_entry_timestamp_to_iso():
    """
    Test _convert_arxiv_file_timestamp_to_iso method for converting EST file timestamp to ISO 8601 GMT.
    """
    # Input timestamp in EST
    input_timestamp = '2010-12-23 00:13:59'
    # Expected output in GMT
    expected_output = '2010-12-23T05:13:59+00:00'

    # Call the method
    result = Manifest._convert_arxiv_file_entry_timestamp_to_iso(input_timestamp)

    # Assert the result matches the expected output
    assert result == expected_output


def test_convert_arxiv_timestamp_to_iso_invalid_format():
    """
    Test _convert_arxiv_timestamp_to_iso method with an invalid timestamp format.
    """
    # Invalid input timestamp
    input_timestamp = 'Invalid Timestamp'

    # Assert that a ValueError is raised
    with pytest.raises(ValueError):
        Manifest._convert_arxiv_timestamp_to_iso(input_timestamp)


def test_convert_arxiv_file_entry_timestamp_to_iso_invalid_format():
    """
    Test _convert_arxiv_file_timestamp_to_iso method with an invalid timestamp format.
    """
    # Invalid input timestamp
    input_timestamp = 'Invalid Timestamp'

    # Assert that a ValueError is raised
    with pytest.raises(ValueError):
        Manifest._convert_arxiv_file_entry_timestamp_to_iso(input_timestamp)


def test_is_file_entry_consistent_valid_entry():
    """
    Test _is_file_entry_consistent with a valid file entry.
    """
    valid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1508_002.tar',
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1508'
    }
    assert Manifest._is_file_entry_consistent(valid_entry) is True


def test_is_file_entry_consistent_invalid_filename():
    """
    Test _is_file_entry_consistent with an invalid filename.
    """
    invalid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/invalid_filename.tar',
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1508'
    }
    assert Manifest._is_file_entry_consistent(invalid_entry) is False

def test_is_file_entry_consistent_invalid_month():
    """
    Test _is_file_entry_consistent with an invalid month in 'yymm'.
    """
    invalid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1508_002.tar',
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1513'  # Invalid month
    }
    assert Manifest._is_file_entry_consistent(invalid_entry) is False

def test_is_file_entry_consistent_invalid_seq_num():
    """
    Test _is_file_entry_consistent with an invalid sequence number in the filename.
    """
    invalid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1508_003.tar',  # seq_num mismatch
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1508'
    }
    assert Manifest._is_file_entry_consistent(invalid_entry) is False

def test_process_file_entry_valid():
    """
    Test _process_file_entry with a valid file entry.
    """
    valid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1508_002.tar',
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1508'
    }

    processed_entry = Manifest._process_file_entry(valid_entry)

    assert processed_entry == {
        'filename': 'src/arXiv_src_1508_002.tar',
        'size_bytes': 537749445,
        'timestamp_iso': '2017-08-05T10:13:16+00:00',
        'year': 2015,
        'month': 8,
        'sequence_number': 2,
        'n_submissions': 438,
        'hash': {
            'MD5': '271195f030a45b84d397dc8c540bde7f',
            'MD5_contents': '5f4774a944c17e67f334ebb9bf912dbf',
        }
    }

def test_process_file_entry_inconsistent():
    """
    Test _process_file_entry with an inconsistent file entry.
    """
    inconsistent_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1508_003.tar',  # seq_num mismatch
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1508'
    }

    with pytest.raises(ValueError, match="Entry inconsistent"):
        Manifest._process_file_entry(inconsistent_entry)

def test_process_file_entry_invalid_yymm():
    """
    Test _process_file_entry with an invalid 'yymm' value.
    """
    invalid_entry = {
        'content_md5sum': '5f4774a944c17e67f334ebb9bf912dbf',
        'filename': 'src/arXiv_src_1513_002.tar',  # Invalid month
        'first_item': '1508.00577',
        'last_item': '1508.01014',
        'md5sum': '271195f030a45b84d397dc8c540bde7f',
        'num_items': '438',
        'seq_num': '2',
        'size': '537749445',
        'timestamp': '2017-08-05 06:13:16',
        'yymm': '1513'
    }

    with pytest.raises(ValueError, match="Entry inconsistent"):
        Manifest._process_file_entry(invalid_entry)

@pytest.fixture
def valid_xml_file():
    """
    Create a temporary XML file with valid arXiv data for testing.
    """
    xml_content = """<arXivSRC>
        <timestamp>Mon Apr  7 04:58:03 2025</timestamp>
        <file>
            <content_md5sum>cacbfede21d5dfef26f367ec99384546</content_md5sum>
            <filename>src/arXiv_src_0001_001.tar</filename>
            <first_item>astro-ph0001001</first_item>
            <last_item>quant-ph0001119</last_item>
            <md5sum>949ae880fbaf4649a485a8d9e07f370b</md5sum>
            <num_items>2364</num_items>
            <seq_num>1</seq_num>
            <size>225605507</size>
            <timestamp>2010-12-23 00:13:59</timestamp>
            <yymm>0001</yymm>
        </file>
        <file>
            <content_md5sum>d90df481661ccdd7e8be883796539743</content_md5sum>
            <filename>src/arXiv_src_0002_001.tar</filename>
            <first_item>astro-ph0002001</first_item>
            <last_item>quant-ph0002094</last_item>
            <md5sum>4592ab506cf775afecf4ad560d982a00</md5sum>
            <num_items>2365</num_items>
            <seq_num>1</seq_num>
            <size>227036528</size>
            <timestamp>2010-12-23 00:18:09</timestamp>
            <yymm>0002</yymm>
        </file>
    </arXivSRC>"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as temp_file:
        temp_file.write(xml_content.encode('utf-8'))
        temp_file_path = temp_file.name
    yield temp_file_path
    os.remove(temp_file_path)

def test_import_arxiv_xml_valid(valid_xml_file):
    """
    Test import_arxiv_xml with a valid XML file.
    """
    manifest = Manifest()
    manifest.import_arxiv_xml(valid_xml_file)

    assert manifest._manifest['metadata'] == {
        'manifest_timestamp_iso': '2025-04-07T08:58:03+00:00'
    }
    assert len(manifest._manifest['contents']) == 2
    assert manifest._manifest['contents']['arXiv_src_0001_001.tar']['filename'] == 'src/arXiv_src_0001_001.tar'
    assert manifest._manifest['contents']['arXiv_src_0002_001.tar']['filename'] == 'src/arXiv_src_0002_001.tar'

def test_import_arxiv_xml_file_not_found():
    """
    Test import_arxiv_xml when the file does not exist.
    """
    manifest = Manifest()

    with pytest.raises(FileNotFoundError, match='file not found'):
        manifest.import_arxiv_xml('non_existent_file.xml')

def test_import_arxiv_xml_invalid_structure():
    """
    Test import_arxiv_xml with an invalid XML structure.
    """
    invalid_xml_content = """<invalid></invalid>"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as temp_file:
        temp_file.write(invalid_xml_content.encode('utf-8'))
        temp_file_path = temp_file.name

    manifest = Manifest()

    with pytest.raises(TypeError, match='Entries missing in arXiv XML file'):
        manifest.import_arxiv_xml(temp_file_path)

    os.remove(temp_file_path)

def test_import_arxiv_xml_inconsistent_entry():
    """
    Test import_arxiv_xml with an inconsistent file entry.
    """
    inconsistent_xml_content = """<arXivSRC>
        <timestamp>Mon Apr  7 04:58:03 2025</timestamp>
        <file>
            <content_md5sum>d90df481661ccdd7e8be883796539743</content_md5sum>
            <filename>src/arXiv_src_0002_001.tar</filename>
            <first_item>astro-ph0002001</first_item>
            <last_item>quant-ph0002094</last_item>
            <md5sum>4592ab506cf775afecf4ad560d982a00</md5sum>
            <num_items>2365</num_items>
            <seq_num>1</seq_num>
            <size>227036528</size>
            <timestamp>2010-12-23 00:18:09</timestamp>
            <yymm>0002</yymm>
        </file>
        <file>
            <content_md5sum>cacbfede21d5dfef26f367ec99384546</content_md5sum>
            <filename>invalid_filename.tar</filename>
            <first_item>astro-ph0001001</first_item>
            <last_item>quant-ph0001119</last_item>
            <md5sum>949ae880fbaf4649a485a8d9e07f370b</md5sum>
            <num_items>2364</num_items>
            <seq_num>1</seq_num>
            <size>225605507</size>
            <timestamp>2010-12-23 00:13:59</timestamp>
            <yymm>0001</yymm>
        </file>        
    </arXivSRC>"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as temp_file:
        temp_file.write(inconsistent_xml_content.encode('utf-8'))
        temp_file_path = temp_file.name

    manifest = Manifest()

    with pytest.raises(ValueError, match='Entry inconsistent'):
        manifest.import_arxiv_xml(temp_file_path)

    os.remove(temp_file_path)

@pytest.fixture
def manifest_with_data():
    """
    Fixture to provide a Manifest instance with preloaded data.
    """
    manifest = Manifest()
    manifest._manifest = {
        'metadata': {
            'manifest_filename': '20250411_arXiv_src_manifest.xml',
            'timestamp_iso': '2025-04-07T08:58:03+00:00'
        },
        'contents': {
            'src/arXiv_src_0001_001.tar': {
             'filename': 'src/arXiv_src_0001_001.tar',
             'size_bytes': 225605507,
             'timestamp_iso': '2010-12-23T05:13:59+00:00',
             'year': 2000,
             'month': 1,
             'sequence_number': 1,
             'n_submissions': 2364,
             'hash': {'MD5': '949ae880fbaf4649a485a8d9e07f370b',
                      'MD5_contents': 'cacbfede21d5dfef26f367ec99384546'}},
            'src/arXiv_src_0002_001.tar': {
             'filename': 'src/arXiv_src_0002_001.tar',
             'size_bytes': 227036528,
             'timestamp_iso': '2010-12-23T05:18:09+00:00',
             'year': 2000,
             'month': 2,
             'sequence_number': 1,
             'n_submissions': 2365,
             'hash': {'MD5': '4592ab506cf775afecf4ad560d982a00',
                      'MD5_contents': 'd90df481661ccdd7e8be883796539743'}},
            'src/arXiv_src_0003_001.tar': {
             'filename': 'src/arXiv_src_0003_001.tar',
             'size_bytes': 230986882,
             'timestamp_iso': '2010-12-23T05:22:15+00:00',
             'year': 2000,
             'month': 3,
             'sequence_number': 1,
             'n_submissions': 2600,
             'hash': {'MD5': 'b5bf5e52ae8532cdf82b606b42df16ea',
                      'MD5_contents': '3388afd7bfb2dfd9d3f3e6b353357b33'}},
            'src/arXiv_src_0004_001.tar': {
             'filename': 'src/arXiv_src_0004_001.tar',
             'size_bytes': 191559408,
             'timestamp_iso': '2010-12-23T05:26:31+00:00',
             'year': 2000,
             'month': 4,
             'sequence_number': 1,
             'n_submissions': 2076,
             'hash': {'MD5': '9bf1b55890dceec9535ef723a2aea16b',
                      'MD5_contents': '46abb309d77065fed44965cc26a4ae2e'}},
            'src/arXiv_src_0005_001.tar': {
             'filename': 'src/arXiv_src_0005_001.tar',
             'size_bytes': 255509072,
             'timestamp_iso': '2010-12-23T05:30:11+00:00',
             'year': 2000,
             'month': 5,
             'sequence_number': 1,
             'n_submissions': 2724,
             'hash': {'MD5': 'b49af416746146eca13c5a6a76bc7193',
                      'MD5_contents': 'ea665c7b62eaac91110fa344f6ba3fc4'}}
        }
    }
    return manifest

def test_get_statistics(manifest_with_data):
    """
    Test the get_statistics method to ensure it correctly aggregates data.
    """
    manifest = manifest_with_data
    statistics = manifest.get_statistics()

    # Expected statistics
    expected_statistics = {
        (2000, 1): {'size_bytes': 225605507, 'n_submissions': 2364},
        (2000, 2): {'size_bytes': 227036528, 'n_submissions': 2365},
        (2000, 3): {'size_bytes': 230986882, 'n_submissions': 2600},
        (2000, 4): {'size_bytes': 191559408, 'n_submissions': 2076},
        (2000, 5): {'size_bytes': 255509072, 'n_submissions': 2724},
    }

    assert statistics == expected_statistics

def test_get_statistics_empty_manifest():
    """
    Test the get_statistics method with an empty manifest.
    """
    manifest = Manifest()
    manifest._manifest = {'metadata': {}, 'contents': dict()}
    statistics = manifest.get_statistics()

    assert statistics == {}

@pytest.fixture
def manifest_with_duplicate_keys():
    """
    Fixture to provide a Manifest instance with duplicate (year, month) keys.
    """
    manifest = Manifest()
    manifest._manifest = {
        'metadata': {
            'manifest_filename': '20250411_arXiv_src_manifest.xml',
            'timestamp_iso': '2025-04-07T08:58:03+00:00'
        },
        'contents': {
            'src/arXiv_src_0001_001.tar': {
             'filename': 'src/arXiv_src_0001_001.tar',
             'size_bytes': 225605507,
             'timestamp_iso': '2010-12-23T05:13:59+00:00',
             'year': 2025,
             'month': 1,
             'sequence_number': 1,
             'n_submissions': 2364},
            'src/arXiv_src_0002_001.tar': {
             'filename': 'src/arXiv_src_0002_001.tar',
             'size_bytes': 227036528,
             'timestamp_iso': '2010-12-23T05:18:09+00:00',
             'year': 2025,
             'month': 1,
             'sequence_number': 2,
             'n_submissions': 1000},
        }
    }
    return manifest

def test_get_statistics_branch_coverage(manifest_with_duplicate_keys):
    """
    Test get_statistics to ensure both branches of the conditional are covered.
    """
    manifest = manifest_with_duplicate_keys
    statistics = manifest.get_statistics()

    # Expected statistics
    expected_statistics = {
        (2025, 1): {
            'size_bytes': 225605507 + 227036528,  # Sum of sizes for the same (year, month)
            'n_submissions': 2364 + 1000          # Sum of submissions for the same (year, month)
        }
    }

    assert statistics == expected_statistics

@pytest.fixture
def mock_statistics():
    """
    Fixture to provide mock statistics data for testing.
    """
    return {
        (2025, 1): {'size_bytes': 225605507, 'n_submissions': 2364},
        (2025, 2): {'size_bytes': 227036528, 'n_submissions': 2365},
        (2025, 3): {'size_bytes': 230986882, 'n_submissions': 2600},
    }

@pytest.fixture(autouse=True)
def use_agg_backend():
    """
    Automatically set the matplotlib backend to 'Agg' for all tests.
    This prevents plots from being displayed during testing.
    """
    plt.switch_backend('Agg')

def test_plot_summary_statistics(monkeypatch, mock_statistics):
    """
    Test the plot_summary_statistics method by mocking the output of get_statistics.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        # Create a Manifest instance
        manifest = Manifest()

        # Mock the get_statistics method
        def mock_get_statistics(self):
            return mock_statistics

        monkeypatch.setattr(Manifest, "get_statistics", mock_get_statistics)

        # Call the method
        manifest.plot_summary_statistics()

        # Get all active figures
        figures = [plt.figure(i) for i in plt.get_fignums()]

        # Ensure three figures were created
        assert len(figures) == 3

        # Check the titles of the plots
        expected_titles = [
            'Number of Submissions per Month',
            'Size in GB per Month',
            'Averaged Monthly Submission Size in MB'
        ]
        for fig, expected_title in zip(figures, expected_titles):
            assert fig.axes[0].get_title() == expected_title

        # Check the x-axis labels
        expected_xlabels = ['Date (Year-Month)', 'Date (Year-Month)', 'Date (Year-Month)']
        for fig, expected_xlabel in zip(figures, expected_xlabels):
            assert fig.axes[0].get_xlabel() == expected_xlabel

        # Check the y-axis labels
        expected_ylabels = ['Number of Submissions', 'Size (GB)', 'Average Submission Size (MB)']
        for fig, expected_ylabel in zip(figures, expected_ylabels):
            assert fig.axes[0].get_ylabel() == expected_ylabel

        # Clean up the figures after the test
        plt.close('all')


def test_info(mocker):
    """
    Test the info() method of the Manifest class.
    """

    # Create a mock Manifest instance with test data
    manifest = Manifest()
    manifest._manifest = {
        'metadata': {
            'manifest_timestamp_iso': '2025-04-07T08:58:03+00:00',
        },
        'contents': {
            'src/arXiv_src_0001_001.tar': {
                'filename': 'src/arXiv_src_0001_001.tar',
                'size_bytes': 225605507,
                'timestamp_iso': '2010-12-23T05:13:59+00:00',
                'year': 2000,
                'month': 1,
                'sequence_number': 1,
                'n_submissions': 2364,
                'hash': {
                    'MD5': '949ae880fbaf4649a485a8d9e07f370b',
                    'MD5_contents': 'cacbfede21d5dfef26f367ec99384546'
                }
            },
            'src/arXiv_src_0002_001.tar': {
                'filename': 'src/arXiv_src_0002_001.tar',
                'size_bytes': 227036528,
                'timestamp_iso': '2010-12-23T05:18:09+00:00',
                'year': 2000,
                'month': 2,
                'sequence_number': 1,
                'n_submissions': 2365,
                'hash': {
                    'MD5': '4592ab506cf775afecf4ad560d982a00',
                    'MD5_contents': 'd90df481661ccdd7e8be883796539743'
                }
            }
        }
    }

    # Mock the print function to capture output
    mock_print = mocker.patch("builtins.print")

    # Call the info() method
    manifest.info()

    # Verify the printed output in the correct order
    expected_calls = [
        mocker.call("Manifest Information:"),
        mocker.call("Metadata:"),
        mocker.call("  Manifest Timestamp: 2025-04-07T08:58:03+00:00"),
        mocker.call("Number of Bulk Archives: 2"),
        mocker.call("Total Number of Submissions: 4729"),
        mocker.call("Total Size: 0.453 GB"),
        mocker.call("Average Submission Size: 0.096 MB"),
    ]

    mock_print.assert_has_calls(expected_calls, any_order=False)

def test_info_with_empty_metadata(capsys):
    """
    Test the info() method when metadata is empty.
    """
    manifest = Manifest()
    manifest._manifest = {
        'metadata': {},  # Empty metadata
        'contents': [
            {
                'filename': 'src/arXiv_src_0001_001.tar',
                'size_bytes': 225605507,
                'timestamp_iso': '2010-12-23T05:13:59+00:00',
                'year': 2000,
                'month': 1,
                'sequence_number': 1,
                'n_submissions': 2364,
                'hash': {
                    'MD5': '949ae880fbaf4649a485a8d9e07f370b',
                    'MD5_contents': 'cacbfede21d5dfef26f367ec99384546'
                }
            }
        ]
    }

    # Call the info() method
    manifest.info()

    # Capture the printed output
    captured = capsys.readouterr()

    # Assert that no output is printed for metadata
    assert captured.out.strip() == ""

def test_manifest_init_with_filename(monkeypatch):
    """
    Test Manifest.__init__ when a filename is provided.
    Ensures import_arxiv_xml is called with the correct argument.
    """
    called = {}

    def mock_import_arxiv_xml(self, file_path):
        called['file_path'] = file_path

    monkeypatch.setattr(Manifest, "import_arxiv_xml", mock_import_arxiv_xml)

    test_path = "dummy/path/arXiv_src_manifest.xml"
    manifest = Manifest(arxiv_xml_file=test_path)

    assert called['file_path'] == test_path

def test_import_arxiv_xml_invalid_format(monkeypatch, tmp_path):
    """
    Test that import_arxiv_xml raises TypeError if the file is not a valid XML format.
    """
    # Create a dummy file
    file_path = tmp_path / "not_xml.txt"
    file_path.write_text("not xml content")

    # Patch XmlHandler.is_xml_format to return False
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.XmlHandler.is_xml_format", lambda x: False)

    manifest = Manifest()
    with pytest.raises(TypeError, match="file is not in XML format."):
        manifest.import_arxiv_xml(str(file_path))

class DummyFileSystemForDuplicate:
    @staticmethod
    def is_file(path):
        return True

class DummyXmlHandlerForDuplicate:
    @staticmethod
    def is_xml_format(path):
        return True

    @staticmethod
    def read_xml_to_dict(path):
        # Two entries with the same filename to trigger the KeyError
        return {
            'arXivSRC': {
                'timestamp': 'Mon Apr  7 04:58:03 2025',
                'file': [
                    {
                        'filename': 'src/arXiv_src_2504_001.tar',
                        'size': '123',
                        'timestamp': '2025-04-07 04:58:03',
                        'yymm': '2504',
                        'seq_num': '001',
                        'num_items': '10',
                        'md5sum': 'abc',
                        'content_md5sum': 'def',
                        'first_item': '1',
                        'last_item': '10'
                    },
                    {
                        'filename': 'src/arXiv_src_2504_001.tar',  # Duplicate filename
                        'size': '456',
                        'timestamp': '2025-04-07 05:00:00',
                        'yymm': '2504',
                        'seq_num': '001',
                        'num_items': '20',
                        'md5sum': 'ghi',
                        'content_md5sum': 'jkl',
                        'first_item': '11',
                        'last_item': '30'
                    }
                ]
            }
        }

@pytest.fixture
def patch_manifest_dependencies_for_duplicate(monkeypatch):
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem", DummyFileSystemForDuplicate)
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.XmlHandler", DummyXmlHandlerForDuplicate)

def test_import_arxiv_xml_duplicate_filename_raises_keyerror(patch_manifest_dependencies_for_duplicate):
    """
    Test that Manifest.import_arxiv_xml raises KeyError when two file entries have identical filenames.
    This hits the branch where duplicate filenames are detected.
    """
    manifest = Manifest()
    with pytest.raises(KeyError, match="Bulk archive base filenames not unique"):
        manifest.import_arxiv_xml("dummy_path.xml")

def test_list_keys():
    """
    Test listing all keys in the manifest.
    """

    manifest = Manifest()
    manifest._manifest['contents']['key1'] = {"foo": "bar"}
    manifest._manifest['contents']['key2'] = {"baz": "qux"}
    keys = manifest.list_keys()
    assert keys == {"key1", "key2"}

class DummyTimeService:
    @staticmethod
    def is_iso_timestamp_newer(ts1, ts2):
        # Simple string comparison for testing; real logic is in TimeService
        return ts1 > ts2

@pytest.fixture(autouse=True)
def patch_time_service(monkeypatch):
    # Patch TimeService used in Manifest
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.TimeService", DummyTimeService)

def make_manifest(timestamp, keys):
    """
    Helper to create a Manifest with a given timestamp and set of keys.
    """
    m = Manifest()
    m._manifest['metadata']['manifest_timestamp_iso'] = timestamp
    m._manifest['contents'] = {k: {} for k in keys}
    return m

def test_is_newer_than_true():
    """
    Test that is_newer_than returns True when self is newer and has at least one new entry, no deletions.
    """
    m1 = make_manifest("2022-01-02T00:00:00+00:00", {"a", "b"})
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"a"})
    assert m1.is_newer_than(m2) is True

def test_is_newer_than_false():
    """
    Test that is_newer_than returns False when self is older and the other has at least one new entry, no deletions.
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a"})
    m2 = make_manifest("2022-01-02T00:00:00+00:00", {"a", "b"})
    assert m1.is_newer_than(m2) is False

def test_is_newer_than_equal():
    """
    Test that is_newer_than returns False when timestamps and keys are identical.
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})
    assert m1.is_newer_than(m2) is False

def test_is_newer_than_identical_time_different_keys():
    """
    Test that is_newer_than raises ValueError if timestamps are equal but keys differ.
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"a"})
    with pytest.raises(ValueError, match="identical times must have identical keys"):
        m1.is_newer_than(m2)

def test_is_newer_than_newer_manifest_no_new_entries():
    """
    Test that is_newer_than raises ValueError if self is newer but has no new entries.
    """
    m1 = make_manifest("2022-01-02T00:00:00+00:00", {"a"})
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"a"})
    with pytest.raises(ValueError, match="must have at least one new entry"):
        m1.is_newer_than(m2)

def test_is_newer_than_newer_manifest_with_deletions():
    """
    Test that is_newer_than raises ValueError if self is newer but has deleted entries.
    """
    m1 = make_manifest("2022-01-02T00:00:00+00:00", {"a"})         # newer, but missing "b"
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})    # older, has "a" and "b"
    with pytest.raises(ValueError, match="Inconsistent manifest metadata, newer manifest must have at least one new entry"):
        m1.is_newer_than(m2)

def test_is_newer_than_older_manifest_no_new_entries():
    """
    Test that is_newer_than raises ValueError if self is older but the other has no new entries.
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a"})
    m2 = make_manifest("2022-01-02T00:00:00+00:00", {"a"})
    with pytest.raises(ValueError, match="must have at least one new entry"):
        m1.is_newer_than(m2)

def test_is_newer_than_older_manifest_with_deletions():
    """
    Test that is_newer_than raises ValueError if self is older but has deleted entries compared to the other.
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})
    m2 = make_manifest("2022-01-02T00:00:00+00:00", {"a"})
    with pytest.raises(ValueError, match="Inconsistent manifest metadata, newer manifest must have at least one new entry"):
        m1.is_newer_than(m2)

def test_is_newer_than_newer_manifest_new_entries_but_deleted_key():
    """
    Test that is_newer_than raises ValueError (line 154) if self is newer but has no new entries compared to other,
    and both have different keys (e.g., self: {'a', 'b'}, other: {'b', 'c'}).
    """
    m1 = make_manifest("2022-01-02T00:00:00+00:00", {"a", "b"})  # newer
    m2 = make_manifest("2022-01-01T00:00:00+00:00", {"b", "c"})  # older
    with pytest.raises(ValueError, match="cannot have entries deleted"):
        m1.is_newer_than(m2)

def test_is_newer_than_older_manifest_deleted_key_but_newer_entries():
    """
    Test that is_newer_than raises ValueError (line 154) if self is newer but has no new entries compared to other,
    and both have different keys (e.g., self: {'a', 'b'}, other: {'b', 'c'}).
    """
    m1 = make_manifest("2022-01-01T00:00:00+00:00", {"a", "b"})  # newer
    m2 = make_manifest("2022-01-02T00:00:00+00:00", {"b", "c"})  # older
    with pytest.raises(ValueError, match="cannot have entries deleted"):
        m1.is_newer_than(m2)

class UniqueDummyTimeService:
    @staticmethod
    def is_iso_timestamp_newer(ts1, ts2):
        return ts1 > ts2

@pytest.fixture
def patch_time_service_unique(monkeypatch):
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.TimeService", UniqueDummyTimeService)

def make_manifest_with_hashes_unique(timestamp, entries):
    """
    Helper to create a Manifest with a given timestamp and a dict of entries:
    entries: dict of {filename: md5_hash}
    """
    m = Manifest()
    m._manifest['metadata']['manifest_timestamp_iso'] = timestamp
    m._manifest['contents'] = {
        k: {'hash': {'MD5': v}} for k, v in entries.items()
    }
    return m

def test_find_new_entries_returns_new_keys_unique(patch_time_service_unique):
    """
    Test that find_new_entries returns the set of keys present in self but not in the reference manifest.
    """
    m1 = make_manifest_with_hashes_unique("2022-01-02T00:00:00+00:00", {"a": "h1", "b": "h2"})
    m2 = make_manifest_with_hashes_unique("2022-01-01T00:00:00+00:00", {"a": "h1"})
    assert m1.find_new_entries(m2) == {"b"}

def test_find_new_entries_raises_if_not_newer_unique(patch_time_service_unique):
    """
    Test that find_new_entries raises ValueError if self is not newer than the reference manifest.
    """
    m1 = make_manifest_with_hashes_unique("2022-01-01T00:00:00+00:00", {"a": "h1"})
    m2 = make_manifest_with_hashes_unique("2022-01-02T00:00:00+00:00", {"a": "h1", "b": "h2"})
    with pytest.raises(ValueError, match="Reference manifest must be older"):
        m1.find_new_entries(m2)

def test_find_updated_entries_returns_changed_keys_unique(patch_time_service_unique):
    """
    Test that find_updated_entries returns the set of keys present in both manifests but with different MD5 hashes.
    """
    # m1 is newer, has a new key "c" and "b" is changed
    m1 = make_manifest_with_hashes_unique("2022-01-02T00:00:00+00:00", {"a": "h1", "b": "h2", "c": "h3"})
    m2 = make_manifest_with_hashes_unique("2022-01-01T00:00:00+00:00", {"a": "h1", "b": "DIFFERENT"})
    assert m1.find_updated_entries(m2) == {"b"}

def test_find_updated_entries_empty_if_no_changes_unique(patch_time_service_unique):
    """
    Test that find_updated_entries returns an empty set if all common keys have the same MD5 hash,
    and the newer manifest has at least one new key.
    """
    m1 = make_manifest_with_hashes_unique("2022-01-02T00:00:00+00:00", {"a": "h1", "b": "h2", "c": "h3"})
    m2 = make_manifest_with_hashes_unique("2022-01-01T00:00:00+00:00", {"a": "h1", "b": "h2"})
    assert m1.find_updated_entries(m2) == set()

def test_find_updated_entries_raises_if_not_newer_unique(patch_time_service_unique):
    """
    Test that find_updated_entries raises ValueError if self is not newer than the reference manifest.
    """
    # m2 is newer and has a new key "b"
    m1 = make_manifest_with_hashes_unique("2022-01-01T00:00:00+00:00", {"a": "h1"})
    m2 = make_manifest_with_hashes_unique("2022-01-02T00:00:00+00:00", {"a": "h1", "b": "h2"})
    with pytest.raises(ValueError, match="Reference manifest must be older"):
        m1.find_updated_entries(m2)

def test_plot_summary_statistics_empty(monkeypatch):
    """
    Test plot_summary_statistics when get_statistics returns an empty dict.
    Should not raise or plot anything.
    """
    manifest = Manifest()

    # Mock get_statistics to return empty dict
    monkeypatch.setattr(Manifest, "get_statistics", lambda self: {})

    # Should not raise or plot anything
    manifest.plot_summary_statistics()
    assert plt.get_fignums() == []

def test_list_entries_by_date_exact():
    """
    Test list_entries_by_date for exact year and month match.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'file1': {'year': 2025, 'month': 8, 'other': 1},
        'file2': {'year': 2025, 'month': 7, 'other': 2},
        'file3': {'year': 2024, 'month': 8, 'other': 3},
    }
    results = manifest.list_entries_by_date(2025, 8)
    assert results == ['file1']

def test_list_entries_by_date_from_date_onwards():
    """
    Test list_entries_by_date with is_from_date_onwards=True.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'file1': {'year': 2025, 'month': 8, 'other': 1},
        'file2': {'year': 2025, 'month': 7, 'other': 2},
        'file3': {'year': 2024, 'month': 8, 'other': 3},
        'file4': {'year': 2025, 'month': 9, 'other': 4},
        'file5': {'year': 2026, 'month': 1, 'other': 5},
    }
    results = manifest.list_entries_by_date(2025, 8, is_from_date_onwards=True)
    expected = ['file1', 'file4', 'file5']
    assert results == expected

def test_list_entries_by_date_none():
    """
    Test list_entries_by_date returns empty list if no match.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'file1': {'year': 2025, 'month': 8, 'other': 1},
        'file2': {'year': 2025, 'month': 7, 'other': 2},
    }
    results = manifest.list_entries_by_date(2024, 1)
    assert results == []


def test_list_filenames():
    """
    Test the list_filenames method to ensure it returns all filenames in the manifest.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {
            'filename': 'src/arXiv_src_0001_001.tar',
        },
        'arXiv_src_0002_001.tar': {
            'filename': 'src/arXiv_src_0002_001.tar',
        },
        'arXiv_src_0003_001.tar': {
            'filename': 'src/arXiv_src_0003_001.tar',
        },
    }
    filenames = manifest.list_filenames()
    expected = [
        'arXiv_src_0001_001.tar',
        'arXiv_src_0002_001.tar',
        'arXiv_src_0003_001.tar',
    ]

    assert set(filenames) == set(expected)


def test_find_bulk_archive_files_not_in_manifest(monkeypatch):
    """
    Test find_bulk_archive_files_not_in_manifest returns files in the directory not in the manifest.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'arXiv_src_0002_001.tar'},
    }

    # Mock FileSystem.is_directory to always return True
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    # Mock FileSystem.list_files to return a mix of files and accept the extra argument
    files_in_dir = [
        'arXiv_src_0001_001.tar',  # in manifest
        'arXiv_src_0002_001.tar',  # in manifest
        'arXiv_src_0003_001.tar',  # not in manifest
        'not_a_bulk_file.txt',     # not a bulk archive file
    ]
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: files_in_dir)

    missing = manifest.find_bulk_archive_files_not_in_manifest("/dummy/path")
    assert missing == ['arXiv_src_0003_001.tar']


    """
    Test find_bulk_archive_files_not_in_manifest returns files in the directory not in the manifest.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'arXiv_src_0002_001.tar'},
    }

    # Mock FileSystem.is_directory to always return True
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    # Mock FileSystem.list_files to return a mix of files and accept the extra argument
    files_in_dir = [
        'arXiv_src_0001_001.tar',  # in manifest
        'arXiv_src_0002_001.tar',  # in manifest
        'arXiv_src_0003_001.tar',  # not in manifest
        'not_a_bulk_file.txt',     # not a bulk archive file
    ]
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: files_in_dir)

    missing = manifest.find_bulk_archive_files_not_in_manifest("/dummy/path")
    assert missing == ['arXiv_src_0003_001.tar']

def test_find_bulk_archive_files_not_in_manifest_directory_not_found(monkeypatch):
    """
    Test find_bulk_archive_files_not_in_manifest raises FileNotFoundError if directory does not exist.
    """
    manifest = Manifest()
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: False)
    # Also patch list_files to accept the extra argument, even though it won't be called
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: [])
    with pytest.raises(FileNotFoundError):
        manifest.find_bulk_archive_files_not_in_manifest("/not/a/real/path")


    """
    Test find_bulk_archive_files_not_in_manifest raises FileNotFoundError if directory does not exist.
    """
    manifest = Manifest()
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: False)
    with pytest.raises(FileNotFoundError):
        manifest.find_bulk_archive_files_not_in_manifest("/not/a/real/path")

def test_find_keys_without_local_files_all_present(monkeypatch):
    """
    Test when all files for the given keys exist in the directory.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'src/arXiv_src_0002_001.tar'},
        'arXiv_src_0003_001.tar': {'filename': 'src/arXiv_src_0003_001.tar'},
    }
    # All files are present in the directory (as basenames)
    local_files = ['arXiv_src_0001_001.tar', 'arXiv_src_0002_001.tar', 'arXiv_src_0003_001.tar']
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: local_files)
    key_list = ['arXiv_src_0001_001.tar', 'arXiv_src_0002_001.tar', 'arXiv_src_0003_001.tar']
    missing = manifest.find_keys_without_local_files("/dummy/path", key_list)
    assert missing == []

def test_find_keys_without_local_files_some_missing(monkeypatch):
    """
    Test when some files for the given keys are missing in the directory.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'src/arXiv_src_0002_001.tar'},
        'arXiv_src_0003_001.tar': {'filename': 'src/arXiv_src_0003_001.tar'},
    }
    # Only some files are present in the directory
    local_files = ['arXiv_src_0001_001.tar']
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: local_files)
    key_list = ['arXiv_src_0001_001.tar', 'arXiv_src_0002_001.tar', 'arXiv_src_0003_001.tar']
    missing = manifest.find_keys_without_local_files("/dummy/path", key_list)
    assert set(missing) == {'arXiv_src_0002_001.tar', 'arXiv_src_0003_001.tar'}

def test_find_keys_without_local_files_none_exist(monkeypatch):
    """
    Test when none of the files for the given keys exist in the directory.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'src/arXiv_src_0002_001.tar'},
    }
    # No files are present in the directory
    local_files = []
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: local_files)
    key_list = ['arXiv_src_0001_001.tar', 'arXiv_src_0002_001.tar']
    missing = manifest.find_keys_without_local_files("/dummy/path", key_list)
    assert set(missing) == set(key_list)

def test_find_keys_without_local_files_key_not_in_manifest(monkeypatch):
    """
    Test when a key in key_list is not present in the manifest; it should be ignored.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
    }
    local_files = ['arXiv_src_0001_001.tar']
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: True)
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.list_files", lambda path, include_subdirectories=False: local_files)
    key_list = ['arXiv_src_0001_001.tar', 'arXiv_src_0002_001.tar']  # 2nd key not in manifest
    missing = manifest.find_keys_without_local_files("/dummy/path", key_list)
    assert missing == []

def test_find_keys_without_local_files_directory_not_found(monkeypatch):
    """
    Test that FileNotFoundError is raised if the directory does not exist.
    """
    manifest = Manifest()
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileSystem.is_directory", lambda path: False)
    key_list = ['arXiv_src_0001_001.tar']
    try:
        manifest.find_keys_without_local_files("/not/a/real/path", key_list)
    except FileNotFoundError:
        pass
    else:
        assert False, "Expected FileNotFoundError"

def test_get_bulk_archive_filename_valid(monkeypatch):
    """
    Test get_bulk_archive_filename returns the correct basename for a valid key.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
        'arXiv_src_0002_001.tar': {'filename': 'src/arXiv_src_0002_001.tar'},
    }
    # Patch FileName.get_file_basename to just return the basename
    monkeypatch.setattr("arxiv_bucket.arxiv.manifest.FileName.get_file_basename", lambda f: f.split('/')[-1])
    result = manifest.get_bulk_archive_filename('arXiv_src_0001_001.tar')
    assert result == 'arXiv_src_0001_001.tar'
    result2 = manifest.get_bulk_archive_filename('arXiv_src_0002_001.tar')
    assert result2 == 'arXiv_src_0002_001.tar'

def test_get_bulk_archive_filename_key_not_found():
    """
    Test get_bulk_archive_filename raises KeyError if the key is not in the manifest.
    """
    manifest = Manifest()
    manifest._manifest['contents'] = {
        'arXiv_src_0001_001.tar': {'filename': 'src/arXiv_src_0001_001.tar'},
    }
    with pytest.raises(KeyError, match="not found in the manifest"):
        manifest.get_bulk_archive_filename('arXiv_src_0002_001.tar')

def test_len_empty_manifest():
    """Test __len__ returns 0 for an empty manifest."""
    manifest = Manifest()
    assert len(manifest) == 0

def test_len_with_entries():
    """Test __len__ returns correct count for manifest with entries."""
    manifest = Manifest()
    manifest._manifest['contents']['file1.tar'] = {'filename': 'file1.tar'}
    manifest._manifest['contents']['file2.tar'] = {'filename': 'file2.tar'}
    assert len(manifest) == 2
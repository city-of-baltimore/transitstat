"""Test suite for connector.connector_import"""
from transitstat.connector.data_import import parse_args


def test_parse_sheets():
    """Test for parse_sheets"""


def test_parse_sheets_internal():
    """Test for _parse_shets"""


def test_insert_into_db():
    """Test for insert_into_db"""


def test_parse_args():
    """Tests parse_args"""
    path_str = 'path_str'
    args = parse_args(['-p', path_str])
    assert args.path == path_str

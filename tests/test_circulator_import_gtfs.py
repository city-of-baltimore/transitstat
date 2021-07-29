"""Test suite for circulator.circulator_gtfs"""
from transitstat.circulator.import_gtfs import parse_args


def test_insert_calendar():
    """Test for insert_calendar"""


def test_insert_routes():
    """Test for insert_routes"""


def test_insert_stop_times():
    """Test for insert_stop_times"""


def test_insert_stops():
    """Test for insert_stops"""


def test_get_route_from_stop():
    """Test for get_route_from_stop"""


def test_insert_trips():
    """Test for insert_trips"""


def test_insert():
    """Test for _insert"""


def test_parse_args():
    """Test parse_args"""
    file_str = 'filestr'
    args = parse_args(['-f', file_str, '-r'])
    assert args.recreate
    assert args.file == file_str

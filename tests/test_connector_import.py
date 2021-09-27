"""Test suite for connector.connector_import"""
import shutil
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session  # type: ignore

from transitstat.connector.data_import import parse_args
from transitstat.connector.schema import HcRidership


def test_parse_sheets(tmp_path_factory, connector_import):
    """Test for parse_sheets with a local file"""
    # expected values
    exp_vals = {date(2021, 8, 2): 135, date(2021, 8, 3): 274, date(2021, 8, 4): 250}

    tmp_path = tmp_path_factory.mktemp('data')
    shutil.copy(Path(__file__).parent / 'data' / 'HC3 08-2021.xlsx', tmp_path)
    sheets = connector_import.parse_sheets(tmp_path)
    assert len(sheets.keys()) == 1  # should only have route 3
    assert len(sheets[3].keys()) == 3  # three dates
    for key, val in exp_vals.items():
        assert sheets[3][key] == val

    connector_import.insert_into_db(sheets)

    with Session(bind=connector_import.engine, future=True) as session:
        ret = session.query(HcRidership)
        for hc_rec in ret.all():
            assert exp_vals[hc_rec.date] == hc_rec.riders


def test_parse_args():
    """Tests parse_args"""
    path_str = 'path_str'
    args = parse_args(['-p', path_str])
    assert args.path == path_str

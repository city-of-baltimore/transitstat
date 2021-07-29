"""Tests circulator.import_ridership"""
import shutil
from pathlib import Path

from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

from transitstat.circulator.schema import CirculatorRidershipXLS  # type: ignore
from transitstat.circulator.import_ridership import parse_args


def test_import_ridership_file(dataimporter):
    """Test import_ridership"""
    assert dataimporter.import_ridership(file=Path(__file__).parent.absolute() / 'data' / 'testdata.xlsx')

    engine = create_engine(dataimporter.conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        ret = session.query(CirculatorRidershipXLS).all()
        assert len(ret) == 480


def test_import_ridership_validate_dates(dataimporter):
    """Tests that there are actually dates in the header"""
    assert not dataimporter.import_ridership(file=Path(__file__).parent.absolute() / 'data' / 'testdata3.xlsx')


def test_import_ridership_dir(dataimporter, tmp_path_factory):
    """Test import_ridership"""
    working_dir = tmp_path_factory.mktemp('data')
    shutil.copy(Path(__file__).parent / 'data' / 'testdata.xlsx', working_dir)
    shutil.copy(Path(__file__).parent / 'data' / 'testdata2.xlsx', working_dir)
    assert dataimporter.import_ridership(directory=working_dir)

    engine = create_engine(dataimporter.conn_str, echo=True, future=True)
    with Session(bind=engine, future=True) as session:
        ret = session.query(CirculatorRidershipXLS).all()
        assert len(ret) == 748


def test_parse_args():
    """Test argument parsing"""
    file_str = 'thisfile'
    dir_str = 'thisdir'
    conn_str = 'thisconnstr'
    args = parse_args(['-f', file_str, '-d', dir_str, '-c', conn_str, '-vv'])
    assert args.file == file_str
    assert args.dir == dir_str
    assert args.conn_str == conn_str
    assert args.debug
    assert not args.verbose

import pytest

import ridesystems

from .. import creds


def test_login():
    ridesystems.Scraper(creds.RIDESYSTEMS_USERNAME, creds.RIDESYSTEMS_PASSWORD)


def test_login_failure():
    with pytest.raises(AssertionError):
        ridesystems.Scraper('invalidusername', 'invalidpassword')

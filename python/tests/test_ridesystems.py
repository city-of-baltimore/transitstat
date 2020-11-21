import pytest

import ..creds
import ..ridesystems

def test_login():
    ridesystems.Ridership(creds.RIDESYSTEMS_USERNAME, creds.RIDESYSTEMS_PASSWORD)

def test_login_failure():
    with pytest.raises(AssertionError):
        ridesystems.Ridership('invalidusername', 'invalidpassword')

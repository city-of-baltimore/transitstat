git clone --branch addmainpy git@github.com:city-of-baltimore/transitstat.git
cd transitstat
git clone git@github.com:city-of-baltimore/ridesystems.git ridesystems-repo
python -m venv venv-transitstat
venv-transitstat\Scripts\python.exe -m pip install --upgrade pip wheel pylint flake8 bandit mypy
venv-transitstat\Scripts\python.exe -m pip install --upgrade ./ridesystems-repo
venv-transitstat\Scripts\python.exe -m pip install -r requirements.txt

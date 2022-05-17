# transitstat

![Tests](https://github.com/city-of-baltimore/transitstat/actions/workflows/tests.yml/badge.svg)  
Transitstat is a one-stop shop to download Ridesystems data and upload it into the COB database. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Installing

Check out the repo using your terminal

```
git clone https://github.com/city-of-baltimore/transitstat.git
```

Setup a virtual environment in the root of the checked out code. It's recommended to name this virtual environment .venv-transitstat since that is already in the .gitignore.

```
python -m venv .venv-transitstat
```

Activate your virtual environment

- Windows: `.venv-transitstat\Scripts\activate`
- Linux: `.venv-transitstat\Scripts\activate.sh`
- Bash: `source .venv-transitstat/Scripts/activate`

Install the setup package

```
python setup.py install
```

The package is setup in your virtual environment. If you want to run the scripts, remember to activate your virtual environment. You can run the different scripts using 

```
python -m <script> (report) (optional arguments)
```


### Example

The following will pull the `otp` report for the Circulator for March 23rd, 2022

```
python -m transitstat.circulator.reports otp -s 2022-03-23 -e 2022-03-23
```

If data already exists for the given date interval and you want to overwrite it, use the `-f` option

```
python -m transitstat.circulator.reports otp -s 2022-03-23 -e 2022-03-23 -f
```

## Running the tests

Run the following command to test the repo. Tox will run the unit tests (pytest), linter (flake8, pylint), static type checker (mypy), security issues checker (bandit), and converage test report.

```
tox
```

### Unit Tests

If you wanted to run a single test, use the following command.

```
tox -e py3 -- -k <test_function>
```

-k searches for a matching string in the test names so 

```
tox -e py3 -- -k test_get_otp
```

would run both the test_get_otp_no_force and test_get_otp_force tests.

You can run all the tests in a test file by specifying the file name.

```
tox -e py3 -- -k test_circulator_reports
```

### Coding style tests

Run the following command to test for code style using the flake8 and pylint linters.

```
tox -e lint
```

Run the following command to test for static typing

```
tox -e mypy
```

## Author

* **Brian Seel** - [cylussec](https://github.com/cylussec)

See also the list of [contributors](https://github.com/city-of-baltimore/transitstat/graphs/contributors) who participated in this project.

## Notes

* This repo makes heavy use of the `ridesystems` library. It could be useful to take a look through the [repo](https://github.com/city-of-baltimore/ridesystems) to understand where the data is coming from and how it is being transformed in the `reports.py` file.
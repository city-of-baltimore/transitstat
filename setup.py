"""This is just for Tox support"""

from setuptools import setup, find_packages

setup(
    name='TransitStat',
    version='0.1',
    author="Brian Seel",
    author_email="brian.seel@baltimorecity.gov",
    description="Interface with the Ridesystems website",
    packages=find_packages('src'),
    package_data={'transitstat': ['py.typed'], },
    package_dir={'': 'src'},
    install_requires=[
        'pandas~=1.3.0',
        'pyodbc~=4.0.31',
        'loguru~=0.5.3',
        'python-dateutil~=2.8.2',
        'tenacity~=8.0.1',
        'sqlalchemy~=1.4.20',
        'openpyxl~=3.0.7',
        'ridesystems @ git+https://github.com/city-of-baltimore/Ridesystems@v2.0.1#egg=ridesystems',
    ]
)

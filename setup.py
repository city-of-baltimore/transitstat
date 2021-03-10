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
        'pandas~=1.2.3',
        'pyodbc~=4.0.30',
        'loguru~=0.5.3',
        'python-dateutil~=2.8.1',
        'ridesystems @ git+git://github.com/city-of-baltimore/Ridesystems@v1.0.0#egg=ridesystems',
    ]
)

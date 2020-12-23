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
        'pandas',
        'pyodbc',
        'http://github.com/city-of-baltimore/Ridesystems#egg=ridesystems',
    ]
)

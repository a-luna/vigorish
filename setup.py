from setuptools import setup, find_packages
from pathlib import Path


HERE = Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name='vigorish-lunaa',
    version='0.1.0',
    description="""Vigorish scrapes and stores MLB data sets, including
        detailed boxscores, pitchfx measurements and player biographical info.
        """,
    long_description=README,
    long_description_content_type="text/markdown",
    keywords='mlb boxscore pitchfx baseball sports',
    url="https://github.com/a-luna/vigorish",
    author='Aaron Luna',
    author_email='aaronluna@gmail.com',
    license="MIT",
    py_modules=['vig'],
    install_requires=[
        'alembic',
        'boto3',
        'Click',
        'fuzzywuzzy',
        'lxml',
        'pandas',
        'psycopg2-binary',
        'python-dotenv',
        'python-levenshtein',
        'requests',
        'selenium',
        'SQLAlchemy',
        'tqdm',
        'tzlocal',
        'urllib3',
        'w3lib'
    ],
    entry_points='''
        [console_scripts]
        vig=vig:cli
    ''',
)
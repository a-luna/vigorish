from setuptools import setup, find_packages
from pathlib import Path


HERE = Path(__file__).parent
README = (HERE / "README.md").read_text()
DESCRIPTION = (
    'Vigorish scrapes and stores MLB data, including detailed boxscores, '
    'pitchfx measurements and player biographical info.'
)

setup(
    name='vigorish',
    version='0.1.0',
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    keywords='baseball mlb scrape boxscore pitchfx',
    url="https://github.com/a-luna/vigorish",
    author='Aaron Luna',
    author_email='aaronluna@gmail.com',
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
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
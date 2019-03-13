from setuptools import setup, find_packages
from pathlib import Path


HERE = Path(__file__).parent
README = (HERE / "README.md").read_text()
DESCRIPTION = (
    'Web scraper for various MLB data sets, including detailed boxscores, '
    'pitchfx measurements and player biographical info.'
)

setup(
    name='vigorish',
    version='0.1.1',
    packages=find_packages(),
    include_package_data=True,
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
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=[
        'alembic',
        'boto3',
        'Click',
        'fuzzywuzzy',
        'halo',
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
        vig=app.cli.vig:cli
    ''',
)
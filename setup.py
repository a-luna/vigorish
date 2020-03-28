"""Installation script for vigorish CLI application."""
from pathlib import Path
from setuptools import setup, find_packages

DESCRIPTION = "MLB data scraping tool"
APP_ROOT = Path(__file__).parent
README = (APP_ROOT / "README.md").read_text()
AUTHOR = "Aaron Luna"
AUTHOR_EMAIL = "admin@aaronluna.dev"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/a-luna/vigorish/issues",
    "Source Code": "https://github.com/a-luna/vigorish",
}
INSTALL_REQUIRES = [
    "alembic",
    "boto3",
    "bullet",
    "Click",
    "dataclass-csv",
    "fake-useragent",
    "fuzzywuzzy",
    "halo",
    "lxml",
    "naked",
    "pandas",
    "py-getch",
    "python-dateutil",
    "python-dotenv",
    "python-levenshtein",
    "requests",
    "selenium",
    "SQLAlchemy",
    "tqdm",
    "tzlocal",
    "urllib3",
    "w3lib",
]
EXTRAS_REQUIRE = {
    "dev": [
        "black",
        "coverage",
        "flake8",
        "ipython",
        "mypy",
        "pip-review",
        "pre-commit",
        "pydocstyle",
        "pytest",
        "pytest-black",
        "pytest-clarity",
        "pytest-cov",
        "pytest-dotenv",
        "pytest-flake8",
        "pytest-mypy",
        "snoop",
        "tox",
    ]
}

setup(
    name="vigorish",
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    version="0.1",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=AUTHOR,
    maintainer_email=AUTHOR_EMAIL,
    license="MIT",
    url="https://github.com/a-luna/vigorish",
    project_urls=PROJECT_URLS,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points="""
        [console_scripts]
        vig=vigorish.cli.vig:cli
    """,
)

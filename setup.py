"""Installation script for vigorish CLI application."""
from pathlib import Path
from setuptools import setup, find_packages

DESCRIPTION = "MLB data scraping tool"
APP_ROOT = Path(__file__).parent
README = (APP_ROOT / "README.md").read_text().strip()
AUTHOR = "Aaron Luna"
AUTHOR_EMAIL = "contact@aaronluna.dev"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/a-luna/vigorish/issues",
    "Source Code": "https://github.com/a-luna/vigorish",
}
CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Environment :: Console",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Natural Language :: English",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: POSIX :: Linux",
    "Operating System :: Unix",
    "Programming Language :: JavaScript",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3 :: Only",
]
INSTALL_REQUIRES = [
    "boto3==1.12.36",
    "bullet==2.1.0",
    "click==7.1.1",
    "dataclass-csv==1.1.3",
    "fake-useragent==0.1.11",
    "fuzzywuzzy==0.18.0",
    "halo==0.0.29",
    "lxml==4.5.0",
    "Naked==0.1.31",
    "pandas==1.0.3",
    "py-getch==1.0.1",
    "python-dateutil==2.8.1",
    "python-dotenv==0.12.0",
    "python-Levenshtein==0.12.0",
    "requests==2.23.0",
    "selenium==3.141.0",
    "SQLAlchemy==1.3.15",
    "tqdm==4.45.0",
    "tzlocal==2.0.0",
    "urllib3==1.25.8",
    "w3lib==1.21.0",
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
    version="0.2.1",
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
    python_requires=">=3.7",
    classifiers=CLASSIFIERS,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points="""
        [console_scripts]
        vig=vigorish.cli.vig:cli
    """,
)

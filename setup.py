"""Installation script for vigorish CLI application."""
from pathlib import Path
from setuptools import setup, find_packages

DESCRIPTION = "Hybrid Python/Node.js web scraper for Major League Baseball (MLB) data."
APP_ROOT = Path(__file__).parent
README = (APP_ROOT / "README.md").read_text().strip()
AUTHOR = "Aaron Luna"
AUTHOR_EMAIL = "contact@aaronluna.dev"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/a-luna/vigorish/issues",
    "Source Code": "https://github.com/a-luna/vigorish",
    "Documentation": "https://aaronluna.dev/projects/vigorish",
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
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3 :: Only",
]
INSTALL_REQUIRES = [
    "aenum",
    "boto3",
    "bullet",
    "click",
    "dacite",
    "dataclass-csv",
    "enlighten",
    "events",
    "rapidfuzz",
    "halo",
    "lxml",
    "naked",
    "py-getch",
    "python-dateutil",
    "python-dotenv",
    "requests",
    "sqlalchemy",
    "sqlalchemy-utils",
    "tabulate",
    "tqdm",
    "urllib3",
    "w3lib",
]
EXTRAS_REQUIRE = {
    "dev": [
        "black",
        "coverage",
        "flake8",
        "pydocstyle",
        "pytest",
        "pytest-black",
        "pytest-clarity",
        "pytest-cov",
        "pytest-dotenv",
        "pytest-flake8",
        "tox",
        "twine",
        "wheel",
    ]
}

setup(
    name="vigorish",
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    version="0.3.3",
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
    classifiers=CLASSIFIERS,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points="""
        [console_scripts]
        vig=vigorish.cli.vig:cli
    """,
)

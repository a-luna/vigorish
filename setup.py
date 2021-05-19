# flake8: noqa
"""Installation script for vigorish CLI application."""
from pathlib import Path

from setuptools import find_packages, setup

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
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3 :: Only",
]
INSTALL_REQUIRES = [
    "aenum",
    "boto3",
    "bullet",
    "click==7.1.2",
    "dacite",
    "dataclass-csv",
    "enlighten",
    "events",
    "halo",
    "lxml",
    "naked",
    "py-getch",
    "pyfiglet",
    "python-dateutil",
    "python-dotenv",
    "rapidfuzz",
    "requests",
    "scipy",
    "sqlalchemy==1.3.23",
    "sqlalchemy-utils",
    "tabulate",
    "tqdm",
    "urllib3",
    "w3lib",
]

exec(open(str(APP_ROOT / "src/vigorish/version.py")).read())
setup(
    name="vigorish",
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    version=__version__,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=AUTHOR,
    maintainer_email=AUTHOR_EMAIL,
    license="MIT",
    url=PROJECT_URLS["Source Code"],
    project_urls=PROJECT_URLS,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.6",
    classifiers=CLASSIFIERS,
    install_requires=INSTALL_REQUIRES,
    entry_points="""
        [console_scripts]
        vig=vigorish.cli.vig:cli
    """,
)

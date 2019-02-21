from setuptools import setup, find_packages

setup(
    name='vigorish',
    version='0.1',
    py_modules=['yourscript'],
    include_package_data=True,
    install_requires=[
        'alembic'
        'boto3',
        'Click',
        'lxml',
        'pandas',
        'python-dotenv',
        'requests',
        'SQLAlchemy',
        'tqdm',
        'urllib3',
        'w3lib'
    ],
    entry_points='''
        [console_scripts]
        vig=vig:cli
    ''',
)
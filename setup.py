from setuptools import setup, find_packages

setup(
    name='vigorish',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'alembic'
        'boto3',
        'Click',
        'lxml',
        'pandas',
        'requests',
        'SQLAlchemy',
        'tqdm',
        'urllib3',
        'w3lib'
    ],
    entry_points='''
        [console_scripts]
        vig=app.main.vig:cli
    ''',
)
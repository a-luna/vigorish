pip install --upgrade pip setuptools wheel
pip uninstall vigorish
pip install .[dev]
pytest tests/test_*
cd src/vigorish/nightmarejs
npm install

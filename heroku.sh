pip install --upgrade pip setuptools wheel
pip uninstall vigorish
pip install .[dev]
pytest
cd src/vigorish/nightmarejs
npm install

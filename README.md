#### Setup

python3 -m venv env
source env/bin/activate
python3 src/main.py


# install dependencies/new dependencies
python3 -m pip install -r requirements.txt 

# run test
PYTHONPATH=/Users/dan/development/projects/harvest/src pytest tests/report_test.py
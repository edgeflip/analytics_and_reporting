INSTALL
=======
* `virtualenv env`
* `source env/bin/activate`
* `pip install -r requirements.txt`
* `ln -s keys_dev.py keys.py`
* python main.py --help
* python worker.py --help

LAUNCH DASHBOARD
================
python main.py --port=8080

LAUNCH IMPORTERS
================
python worker.py --fromRDS
python worker.py --fromDynamo


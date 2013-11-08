INSTALL
=======
* `virtualenv env`
* `source env/bin/activate`
* `pip install -r requirements.txt`
* `ln -s keys_dev.py keys.py`
* python main.py --help
* python worker.py --help

Once you've made a virtualenv and set up your credentials, there
are two processes which handle everything: `main.py` and `worker.py`


LAUNCH DASHBOARD
================
python main.py --port=8080

The dashboard and it's various views.  Fire it up and see:

* [The client dashboard](http://dashboard.edgeflip.com/)
* [A simple tool to query for things by fbid](http://dashboard.edgeflip.com/edgeplorer/)
* [A very crude internal dashboard](http://dashboard.edgeflip.com/edgedash/)

Logging in as a client will give a dashboard for that particular client.  Logging in
as a superuser (probably `edgeflip`) will let you choose from available clients.


LAUNCH IMPORTERS
================
python worker.py --fromRDS
python worker.py --fromDynamo

The import process from RDS is similar to a cron job;  every X minutes, it will iterate
through a list of tables, copy them to redshift through S3, and generate new statistics.
Much of the heavy lifting is done with `table_to_redshift.py`, which can also be used
as a command line tool by itself.

The import process from Dynamo needs to run as some sort of daemon;  every X minutes it
scans Redshift tables to queue up users and edges that need to be extracted from Dynamo,
however it is trying to perpetually pull data from Dynamo at a constant rate.

Getting primaries out of Dynamo takes precedence, as this tells the daemon what edges to
look for, and because their data is more interesting in general.


CREDENTIALS
===========
Credentials to the various databases are stored in a file named `keys.py` which does not
exist by default.  Working examples are in keys_stage.py and keys_dev.py. 

This code only reads from RDS and Dynamo, so it is safe to locally point at production.
Because the sync daemons not only write but drop and replace tables, and Redshift breaks
hard if simultaneous table operations occur, it's somewhat important to never run multiple
sync daemons against the same database.

Redshift instances are also relatively expensive, so "Dev" uses the same Redshift instance 
with a database name of `dev` instead of `edgeflip`.  The only potential concern here is
overwhelming the cluster hardware, but this probably won't happen any time soon.


DEBUG MODE
==========
`main.py` and `worker.py` both accept a boolean commandline option of `debug`.

This is set to True by default, which does the following things:
* Disables logging to syslog
* Disables email error reporting
* Enables printing stack traces in the browser
* The webserver (`main.py`) scans for changes to files and autoreloads/restarts

Ie, in production, launch things with --debug=False


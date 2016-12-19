# SegDB2GraceDB

This simply library provides automatic segment queries triggered by annotations in GraceDB.
In this way, information about segments and data quality flags can be loaded into GraceDB without human intervention.

Some assembly required. In particular, lvalert-seglogic.ini, lvalert-init_seglogic and lvalert-run_seglogic may need tweaking based on your set-up.

-------------------------

## Architecture

The main workhorse is ``bin/seglogic.py``, which ingests a config file and several command line options. 
In particular, if it is not given a specific GraceId through the command line option, it will blocks until it reads ``STDIN`` and expects to read an lvalert message passes as a string through ``STDIN`` (the normal procedure for ``lvalert_listen``).

The Config file dictates which types of queries are performed and the individual parameters for each query. 
It also determines where output will be written and which Data Bases are used (both GraceDb and SegDb).

-------------------------

## Types of Queries

-------------------------

## integration with LVAlert

``seglogic.py`` is launched by ``bin/lvalert-run_seglogic``, which is necessary because ``seglogic.py`` requires a config file and ``lvalert_listen`` does not allow users to specify command line options.
The same configuration is currently used for all events, and the listener is subscribed to all possible pipelines.
It is run with the ``gdb_processor`` LVAlert credentials with ``etc/lvalert-seglogic.ini`` as the ``lvalert_listen`` config.

As currently configured, the listener is launched via submission to Condor on emfollow.ligo.caltech.edu through ``bin/gdb_processor-segDb2grcDb`` and is included in the "default" restart script on emfollow under the gracedb.processor account (/home/gracedb.processor/restart-lvalert_listeners.sh).

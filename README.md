# SegDB2GraceDB

This simply library provides automatic segment queries triggered by annotations in GraceDB.
In this way, information about segments and data quality flags can be loaded into GraceDB without human intervention.

Some assembly required. In particular, lvalert-seglogic.ini, lvalert-init_seglogic and lvalert-run_seglogic may need tweaking based on your set-up.

-------------------------

## Architecture

The main workhorse is ``bin/seglogic.py``, which ingests a config file and several command line options. 
In particular, if it is not given a specific GraceId through the command line option, it will blocks until it reads ``STDIN`` and expects to read an lvalert message passes as a string through ``STDIN`` (the normal procedure for ``lvalert_listen``).

The Config file (``etc/seglogic.ini``) dictates which types of queries are performed and the individual parameters for each query. 
It also determines where output will be written and which Data Bases are used (both GraceDb and SegDb).

### Installation

No formal installation is supported at this time. 
Instead, the repo provides a ``setup.sh`` script which modifies the user's paths to make the code discoverable. 
As currently run, the production configuration files both live within ``etc`` in this repository.

-------------------------

## Types of Queries

``seglogic.py`` peforms 3 basic types of queries.

  - queries for individual DQ Flags,
  - queries for the results from a Veto Definer File, and
  - queries for *all* active segments known within the Data Base.

### Individual Flags

Individual flags are queried separately and serially, with information from each flag uploaded to GraceDb before the next is queried. 
The config file allows users to specify 

  - the amont of time to wait after the end of the requested window before performing the query (``wait``),
  - the amount of time included in the query before the event's gpstime (``look_left``),
  - the amount of time included in the query after the event's gpstime (``look_right``),
  - tag names for the GraceDb log messages for both ligolw segment xml files (``extra_queryTags``) and for generic log messages (``extra_tags``),
  - labels to be applied if the flag is active at any time within the query window (``activeLabels``),
  - labels to be applied if the flag is inactive at any time within the query window (``inactiveLabels``),
  - labels to be applied if the flag actually covers the event's gpstime (``flaggedLabels``),
  - labels to be applied if the flag does not actually cover the event's gpstime (``unflaggedLabels``),
  - and a path used when making local queries for "dmt files" rather than querying SegDb itself (``dmt``).

Note, if ``dmt`` is not provided, the script automatically falls back to querying SegDb.

Queries are currently performed via delgation to ``ligolw_segment_query`` and ``ligolw_segment_query_dqsegdb``.

### Veto Definers

The Veto Definer queries are currently unused because no Veto Definer file was provided by the DetChar group for online queries.


### Queries for All Active Segments

These are currently not used because of a typo in ``ligolow_dq_query_dqsegdb`` which has been fixed in the github repo but not deployed on the clusters.

-------------------------

## integration with LVAlert

``seglogic.py`` is launched by ``bin/lvalert-run_seglogic``, which is necessary because ``seglogic.py`` requires a config file and ``lvalert_listen`` does not allow users to specify command line options.
The same configuration is currently used for all events, and the listener is subscribed to all possible pipelines.
It is run with the ``gdb_processor`` LVAlert credentials with ``etc/lvalert-seglogic.ini`` as the ``lvalert_listen`` config.

As currently configured, the listener is launched via submission to Condor on emfollow.ligo.caltech.edu through ``bin/gdb_processor-segDb2grcDb`` and is included in the "default" restart script on emfollow under the gracedb.processor account (/home/gracedb.processor/restart-lvalert_listeners.sh).

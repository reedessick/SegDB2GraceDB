usage = "segLogic.py [--options] config.ini"
description = "look for segments around a GraceDB event and upload them to GraceDB"
author = "Reed Essick (reed.essick@ligo.org), Peter Shawhan (pshawhan@umd.edu)"


import sys
import os

import time
from lal import gpstime

from glue.ligolw import ligolw
from glue.ligolw import table
from glue.ligolw import lsctables
from glue.ligolw import utils as ligolw_utils

import subprocess as sp

from ConfigParser import SafeConfigParser
from optparse import OptionParser

#=================================================

def flag2filename( flag, start, dur, output_dir="." ):
    return "%s/%s.xml.gz"%(output_dir, flag.replace(":","_"), start, dur)

def segDBcmd( url, flag, start, end, outfilename ):
    ### ligolw_segment_query_dqsegdb -t https://segments.ligo.org -q -a H1:DMT-ANALYSIS_READY:1 -s 1130950800 -e 1131559200
    return "ligolw_segdb_query_dqsegdb -t %s -q -a %s -s %.3f -e %.3f -o %s"%(url, flag, start, end, outfilename)

#=================================================

parser = OptionParser(usage=usage, description=description)

parser.add_option("-v", "--verbose", default=False, action="store_true")

parser.add_option("-g", "--graceid", default=None, type="string", help="if not supplied, looks for lvalert through sys.stdin and blocks")

parser.add_option('-n', '--skip-gracedb-upload', default=False, action='store_true')

opts, args = parser.parse_args()

#========================

if not opts.graceid:
    alert = sys.stdin.read()
    if opts.verbose:
        print "alert received:\n%s"%alert
    alert = json.loads(alert)
    if alert['alert_type'] != 'new':
        if opts.verbose:
            print "alert_type!=new, ignoring..."
        sys.exit(0)
    opts.graceid = alert['uid']

#========================

if len(args)!=1:
    raise ValueError("please exactly one config file as an input argument")

if opts.verbose:
    print "reading config from : %s"%args[0]
config = SafeConfigParser()
config.read( args[0] )

#=================================================

### figure out where we're writing segment files locally
if config.has_option('general', 'output-dir'):
    output_dir = config.get('general', 'output-dir')
    if not os.path.exists(output_dir):
        os.makedirs( output_dir )
else:
    output_dir = "."

### find which GraceDb we're using and pull out parameters of this event
if config.has_option('general', 'gracedb_url'):
    gracedb = GraceDb( config.get('general', 'gracedb_url') )
else:
    gracedb = GraceDb()
event = gracedb.event( opts.graceid ).json()
gpstime = event['gpstime']
if opts.verbose:
    print "processing %s -> %.6f"%(graceid, gpstime)

### find which segDB we're using
if config.has_option('general', 'segdb-url'):
    segdb_url = config.get('general', 'segdb-url')
else:
    segdb_url = 'https://segments.ligo.org'
if opts.verbose:
    print "searching for segments in : %s"%segdb_url

### iterate through flags, uploading each to GraceDB in turn
for flag in config.get( 'general', 'flags' ).split():
    if opts.verbose:
        print "\t%s"%flag

    start = int(gpstime-config.get_float(flag, 'pad_left'))
    end = gpstime+config.get_float(flag, 'pad_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)
    dur = end-start

    wait = end + config.get_float(flag, 'wait') - gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "\t\twaiting %.3f sec"%(wait)
        time.sleep( wait )

    outfilename = flag2filename( flag, start, dur, output_dir)
    cmd = segDBcmd( segdb_url, flag, start, end, outfilename )
    if opts.verbose:
        print "\t\t%s"%cmd
    sp.Popen( cmd.split() ).wait()

    if not opts.skip_gracedb_upload:
        tags = config.get(flag, 'tags').split()
        message = "SegDb query for %s within [%d, %d]"%(flag, start, end)
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( graceid, message=message, filename=outfilename, tags=tags )

        ### process segments
        xmldoc = ligolw_utils.load_filename(outfilename, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))

        sdef = table.get_table(xmldoc, lsctables.SegmentDefTable.tableName)
        ssum = table.get_table(xmldoc, lsctables.SegmentSummaryTable.tableName)
        seg = table.get_table(xmldoc, lsctables.SegmentTable.tableName)

        ### get segdef_id
        segdef_id = next(a.segment_def_id for a in sdef if a.name==flag.split(":")[1])

        ### define the fraction of the time this flag is defined
        ### get list of defined times
        defd = 0.0
        for a in ssum:
            if a.segment_def_id==sedef_id:
                defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns        

        message = "%s defined : %.3f/%d=%.3f%s"%(flag, defd, dur, defd/dur, "%")
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( graceid, message, tags=tags )

        ### define the fraction of the time this flag is active?
        # get list of  segments
        actv = 0.0
        for a in seg:
            if a.segment_def_id==segdef_id:
                actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
        message = "%s active : %.3f/%d=$%.3f%s"%(flag, actv, dur, actv/dur, "%")
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( graceid, message, tags=tags )


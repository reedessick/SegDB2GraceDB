usage = "segLogic.py [--options] config.ini"
description = "look for segments around a GraceDB event and upload them to GraceDB"
author = "Reed Essick (reed.essick@ligo.org"


import sys
import os

import time
from lal import gpstime

import subprocess as sp

from ConfigParser import SafeConfigParser
from optparse import OptionParser

#=================================================

def flag2filename( flag, start, dur, output_dir="." ):
    return "%s/%s.xml.gz"%(flag.replace(":","_"), start, dur)

def segDBcmd( url, flag, start, end, outfilename ):
    return "segdb_query blah blah blah"

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
    wait = gpstime.gps_time_now() - gpstime - config.get_float(flag, 'wait')
    if wait > 0:
        if opts.verbose:
            print "\t\twaiting %.3f sec"%(wait)
        time.sleep( wait )

    start = int(gpstime-config.get_float(flag, 'pad_left'))
    end = gpstime+config.get_float(flag, 'pad_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)

    outfilename = flag2filename( flag, start, end-start, output_dir)
    cmd = segDBcmd( segdb_url, flag, start, end, outfilename )
    if opts.verbose:
        print "\t\t%s"%cmd
    sp.Popen( segDBcmd( segdb_url, flag, start, end, flag2filename( flag, start, end-start, output_dir) ) ).wait()

    if not opts.skip_gracedb_upload:
        message = "SegDb query for %s within [%d, %d]"%(flag, start, end)
        gracedb.writeLog( graceid, message=message, filename=outfilename, tags=config.get(flag, tags).split() )

    ### need to process these into some sort of summary statement?
    ### what logic to use?

### upload to GraceDB








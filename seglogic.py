#!/usr/bin/python
usage       = "segLogic.py [--options] config.ini"
description = "look for segments around a GraceDB event and upload them to GraceDB"
author      = "Reed Essick (reed.essick@ligo.org), Peter Shawhan (pshawhan@umd.edu)"

#-------------------------------------------------

import json
import sys
import os
import glob

from collections import defaultdict

import time
from lal import gpstime as lal_gpstime

from glue.ligolw import ligolw
from glue.ligolw import table
from glue.ligolw import lsctables
from glue.ligolw import utils as ligolw_utils

from ligo.gracedb.rest import GraceDb

import subprocess as sp

from ConfigParser import SafeConfigParser
from optparse import OptionParser

#-------------------------------------------------

def flag2filename( flag, start, dur, output_dir="." ):
    flag = flag.split(":")
    flag = "%s-%s"%(flag[0], "_".join(f.replace("-","_") for f in flag[1:]))
    return "%s/%s-%d-%d.xml.gz"%(output_dir, flag, start, dur)

def segDBcmd( url, flag, start, end, outfilename, dmt=False ):
    ### ligolw_segment_query_dqsegdb -t https://segments.ligo.org -q -a H1:DMT-ANALYSIS_READY:1 -s 1130950800 -e 1131559200
    if dmt:
        return "ligolw_segment_query_dqsegdb --dmt-files -q -a %s -s %d -e %d -o %s"%(flag, start, end, outfilename)
    else:
        return "ligolw_segment_query_dqsegdb -t %s -q -a %s -s %d -e %d -o %s"%(url, flag, start, end, outfilename)

#-----------

def segDBvetoDefcmd( url, vetoDef, start, end, output_dir=".", dmt=False ):
    ### ligolw_segments_from_cats_dqsegdb
    if dmt:
        return "ligolw_segments_from_cats_dqsegdb --dmt-file -v %s -s %d -e %d -i -p -o %s"%(vetoDef, start, end, output_dir)
    else:
        return "ligolw_segments_from_cats_dqsegdb -t %s -v %s -s %d -e %d -i -p -o %s"%(url, vetoDef, start, end, output_dir)

#-----------

def allActivefilename( start, dur, output_dir="."):
    return "%s/allActive-%d-%d.json"%(output_dir, start, dur)

def segDBallActivecmd( url, gps, start_pad, end_pad, outfilename, activeOnly=False ):
    cmd = "ligolw_dq_query_dqsegdb -t %s -s %d -e %d -o %s %d"%(url, start_pad, end_pad, outfilename, gps)
    if activeOnly:
        cmd += " -a"
    return cmd

#-----------

def writeLog( gdb, graceid, message, filename=None, tagname=[] ):
    '''
    delegates to gdb.writeLog but incorporates a common tagname for all uploads
    '''
    gdb.writeLog( graceid, message=message, filename=filename, tagname=['segDb2grcDb']+tagname )

def writeLabel( gdb, graceid, labels ):
    '''
    delegates to gdb.writeLabel but is smart about handling cases where label was already applied
    (turns out GraceDb is very friendly and doesn't raise an error if a label is already present)
    '''
    for label in labels:
        gracedb.writeLabel( opts.graceid, label ) ### GraceDb doesn't raise an error if label is already present, it only warns us

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option("-v", "--verbose", default=False, action="store_true")

parser.add_option("-g", "--graceid", default=None, type="string", help="if not supplied, looks for lvalert through sys.stdin and blocks")

parser.add_option('-n', '--skip-gracedb-upload', default=False, action='store_true')

opts, args = parser.parse_args()

if len(args)!=1:
    raise ValueError("please exactly one config file as an input argument")

#------------------------

### extract data from LVAlert through stdin if needed
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

#------------------------

### read in config file
if opts.verbose:
    print "reading config from : %s"%args[0]
config = SafeConfigParser()
config.read( args[0] )

#-------------------------------------------------

### figure out where we're writing segment files locally
output_dir = config.get('general', 'output-dir')
if not os.path.exists(output_dir):
    os.makedirs( output_dir )

### find which GraceDb we're using and pull out parameters of this event
if config.has_option('general', 'gracedb_url'):
    gracedb = GraceDb( config.get('general', 'gracedb_url') )
else:
    gracedb = GraceDb()

event = gracedb.event( opts.graceid ).json() ### query for this event
gpstime = float(event['gpstime'])
if opts.verbose:
    print "processing %s -> %.6f"%(opts.graceid, gpstime)

### find which segDB we're using
if config.has_option('general', 'segdb-url'):
    segdb_url = config.get('general', 'segdb-url')
else:
    segdb_url = 'https://segments.ligo.org'
if opts.verbose:
    print "searching for segments in : %s"%segdb_url

### figure out global tags and queryTags
g_tags  = config.get('general', 'tags').split()
g_qtags = config.get('general', 'queryTags').split()

### report that we started searching
if not opts.skip_gracedb_upload:
    message = "began searching for segments in : %s"%(segdb_url)
    writeLog( gracedb, opts.graceid, message=message, tagname=g_tags )    

#---------------------------------------------------------------------------------------------------

### iterate through flags, uploading each to GraceDB in turn
flags = config.get( 'general', 'flags' ).split()
flags.sort( key=lambda l: config.getfloat(l,'wait')+config.getfloat(l,'look_right') ) ### sort by how soon we can launch query

for flag in flags:
    if opts.verbose:
        print "    %s"%flag

    ### figure out global tags and queryTags
    tags  = g_tags + config.get(flag, 'extra_tags').split()
    qtags = g_qtags + config.get(flag, 'extra_queryTags').split()

    ### figure out bounds for the query
    start = int(gpstime-config.getfloat(flag, 'look_left'))
    end = gpstime+config.getfloat(flag, 'look_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)
    dur = end-start

    ### set environment for this query
    dmt = config.has_option(flag, 'dmt')
    if dmt:
        os.environ['ONLINEDQ'] = config.get(flag, 'dmt')

    ### wait until data becomes available
    wait = end + config.getfloat(flag, 'wait') - lal_gpstime.gps_time_now()
    if wait > 0:
        if opts.verbose:
            print "        waiting %.3f sec"%(wait)
        time.sleep( wait )

    ### actually perform the query
    outfilename = flag2filename( flag, start, dur, output_dir)
    cmd = segDBcmd( segdb_url, flag, start, end, outfilename, dmt=dmt )
    if opts.verbose:
        print "        %s"%cmd
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()

    ### check returncode for errors
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "\tWARNING: an error occured while querying for this flag!\n%s"%output[1]

        if not opts.skip_gracedb_upload:
            message = "%s<br>&nbsp;&nbsp;<b>WARNING</b>: an error occured while querying for this flag!"%flag
            writeLog( gracedb, opts.graceid, message=message, tagname=qtags )

        continue ### skip the rest, it doesn't make sense to process a non-existant file

    ### report to GraceDb
    if not opts.skip_gracedb_upload:
        ### set up labels
        actvLabels = config.get(flag, 'activeLabels').split()
        inactvLabels = config.get(flag, 'inactiveLabels').split()
        flagLabels = config.get(flag, 'flaggedLabels').split()
        unflagLabels = config.get(flag, 'unflaggedLabels').split()

        ### report query results
        message = "SegDb query for %s within [%d, %d]"%(flag, start, end)
        if opts.verbose:
            print "        %s"%message
        writeLog( gracedb, opts.graceid, message=message, filename=outfilename, tagname=qtags )

        ### process segments into summary statements
        xmldoc = ligolw_utils.load_filename(outfilename, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))

        sdef = table.get_table(xmldoc, lsctables.SegmentDefTable.tableName)
        ssum = table.get_table(xmldoc, lsctables.SegmentSumTable.tableName)
        seg = table.get_table(xmldoc, lsctables.SegmentTable.tableName)

        ### get segdef_id
#        segdef_id = next(a.segment_def_id for a in sdef if a.name==flag.split(":")[1])
        segdef_id = next(a.segment_def_id for a in sdef if a.name=='RESULT')

        ### write message as we process segments
        message = "%s"%flag

        ### define the fraction of the time this flag is defined
        ### get list of defined times
        defd = 0.0
        for a in ssum:
            if a.segment_def_id==segdef_id:
                defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns        
        message += "<br>&nbsp;&nbsp;defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

        ### define the fraction of the time this flag is active?
        # get list of  segments
        actv = 0.0
        flagged = 0
        labels = [] ### labels to be applied
        for a in seg:
            if a.segment_def_id==segdef_id:
                actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                    flagged += 1

        message += "<br>&nbsp;&nbsp;active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")
        if actv:
            if actvLabels:
                message += " <b>Will label as : %s.</b>"%(", ".join(actvLabels))
                labels += actvLabels
        else:
            if inactvLabels:
                message += " <b>Will label as : %s.</b>"%(", ".join(inactvLabels))
                labels += inactvLabels

        if flagged:
            message += "<br>&nbsp;&nbsp;<b>candidate is within these segments!</b>"
            if flagLabels:
                message += " <b>Will label as : %s.</b>"%(", ".join(flagLabels))
                labels += flagLabels
            
        else:
            message += "<br>&nbsp;&nbsp;<b>candidate is not within these segments!</b>"
            if unflagLabels:
                message += " <b>Will label as : %s.</b>"%(", ".join(unflagLabels))
                labels += unflagLabels

        ### post message
        if opts.verbose:
            print "        %s"%message
        writeLog( gracedb, opts.graceid, message, tagname=tags )

        ### apply labels
        writeLabel( gracedb, opts.graceid, set(labels) )

#---------------------------------------------------------------------------------------------------

### iterate through veto definers
vetoDefiners = config.get( 'general', 'vetoDefiners' ).split()
vetoDefiners.sort( key=lambda l: config.getfloat(l,'wait')+config.getfloat(l,'look_right') ) ### sort by how soon we can launch query

for vetoDefiner in vetoDefiners:
    if opts.verbose:
        print "    %s"%vetoDefiner

    ### set up tags
    tags  = g_tags + config.get(vetoDefiner, 'extra_tags').split()
    qtags = g_qtags + config.get(vetoDefiner, 'extra_queryTags').split()

    ### set up GraceDB upload info
    message = "%s"%(vetoDefiner)

    ### figure out query range
    start = int(gpstime-config.getfloat(vetoDefiner, 'look_left'))
    end = gpstime+config.getfloat(vetoDefiner, 'look_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)
    dur = end-start

    ### set environment for this query
    dmt = config.has_option(vetoDefiner, 'dmt')
    if dmt:
        os.environ['ONLINEDQ'] = config.get(vetoDefiner, 'dmt')

    ### set up output dir
    this_output_dir = "%s/%s"%(output_dir, vetoDefiner)
    if not os.path.exists(this_output_dir):
        os.makedirs(this_output_dir)

    ### wait for data to become available
    wait = end + config.getfloat(vetoDefiner, 'wait') - lal_gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "        waiting %.3f sec"%(wait)
        time.sleep( wait )

    ### run segDB query
    cmd = segDBvetoDefcmd( segdb_url, config.get(vetoDefiner, 'path'), start, end, output_dir=this_output_dir, dmt=dmt )
    if opts.verbose:
        print "        %s"%cmd
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()

    ### check return code for errors
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "        WARNING: an error occured while querying for this vetoDefiner!\n%s"%output[1]

        if not opts.skip_gracedb_upload:
            querymessage = "%s<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp<b>WARNING</b>: an error occured while querying for this vetoDefiner!"%flag
            writeLog( gracedb, opts.graceid, message=querymessage, tagname=qtags )

        continue ### skip the rest, it doesn't make sense to process a non-existant file

    ### upload to GraceDb
    if not opts.skip_gracedb_upload:
        ### set up labels
        actvLabels = config.get(vetoDefiner, 'activeLabels').split()
        flagLabels = config.get(vetoDefiner, 'flaggedLabels').split()

        ### collect results of the query and format them into a reasonable data structure
        ### figure out which IFOs are avaiable
        ifos = defaultdict( list )
        for xml in glob.glob("%s/*-VETOTIME_CAT*-%d-%d.xml"%(this_output_dir, start, dur)):
            ifos[os.path.basename(xml).split('-')[0]].append( xml )

        ### figure out which categories are available for each IFO
        for ifo, val in ifos.items():
            cats = defaultdict( list )
            for xml in val:
                cats[os.path.basename(xml).split("-")[1].split("_")[-1]].append( xml )
            ifos[ifo] = cats

        ### iterate through IFOs and through Categories, extracting individual flags and summary statements
        header = "%s"%vetoDefiner
        body = ""
        labels = []
        for ifo in sorted(ifos.keys()):
            if opts.verbose:
                print "    working on IFO : %s"%ifo

            for category in sorted(ifos[ifo].keys()):
                if opts.verbose:
                    print "            working on category : %s"%category

                for xml in ifos[ifo][category]:
                    querymessage = "SegDb query for %s -> %s:%s within [%d, %d]"%(vetoDefiner, ifo, category, start, end)
                    if opts.verbose:
                        print "                %s"%querymessage
                    writeLog( gracedb, opts.graceid, message=querymessage, filename=xml, tagname=qtags )

                    if opts.verbose:
                        print "                reading : %s"%xml
                    xmldoc = ligolw_utils.load_filename(xml, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))
                    
                    sdef = table.get_table(xmldoc, lsctables.SegmentDefTable.tableName)
                    ssum = table.get_table(xmldoc, lsctables.SegmentSumTable.tableName)
                    seg = table.get_table(xmldoc, lsctables.SegmentTable.tableName)

                    vetoDef = table.get_table(xmldoc, lsctables.VetoDefTable.tableName)

                    ### extract info about all flags together (as a category)
                    vetoCATname = 'VETO_%s'%category
                    segdef_id = next(a.segment_def_id for a in sdef if a.name==vetoCATname)

                    header += "<br>&nbsp;&nbsp;%s:%s"%(ifo, category)

                    ### define the fraction of the time this flag is defined
                    ### get list of defined times
                    defd = 0.0
                    for a in ssum:
                        if a.segment_def_id==segdef_id:
                            defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                    header += "<br>&nbsp;&nbsp;&nbsp;&nbsp;defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

                    ### define the fraction of the time this flag is active?
                    # get list of  segments
                    actv = 0.0
                    flagged = 0
                    for a in seg:
                        if a.segment_def_id==segdef_id:
                            actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                            if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                                flagged += 1

                    header += "<br>&nbsp;&nbsp;&nbsp;&nbsp;active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")
                    if actv:
                        if actvLabels:
                            header += " <b>Will label as : %s</b>"%(", ".join(actvLabels))
                            labels += actvLabels

                    if flagged:
                        header += "<br>&nbsp;&nbsp;&nbsp;&nbsp;<b>candidate FAILS %s:%s data quality checks</b>"%(ifo, category)
                        if flagLabels:
                            header += " <b>Will label as : %s.</b>"%(", ".join(flagLabels))
                            labels += flagLabels

                    else:
                        header += "<br>&nbsp;&nbsp;&nbsp;&nbsp;<b>candidate PASSES %s:%s data quality checks</b>"%(ifo, category)

                    ### extract info about individual flags
                    flags = {}
                    for a in sdef: ### map flags to seg_def_id
                        if a.name!=vetoCATname:
                            flags["%s:%s:%s"%(a.ifos, a.name, a.version)] = a.segment_def_id

                    for flag in sorted(flags.keys()): ### analyze each flag individually
                        segdef_id = flags[flag]

                        body += "<br>%s (%s:%s)"%(flag, ifo, category)

                        ### define the fraction of the time this flag is defined
                        ### get list of defined times
                        defd = 0.0
                        for a in ssum:
                            if a.segment_def_id==segdef_id:
                                defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                        body += "<br>&nbsp;&nbsp;defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

                        ### define the fraction of the time this flag is active?
                        # get list of  segments
                        actv = 0.0
                        flagged = 0
                        for a in seg:
                            if a.segment_def_id==segdef_id:
                                actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                                if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                                    flagged += 1

                        body += "<br>&nbsp;&nbsp;active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")

                        if flagged:
                            body += "<br>&nbsp;&nbsp;<b>candidate IS within these segments</b>"

                        else:
                            body += "<br>&nbsp;&nbsp;<b>candidate IS NOT within these segments</b>"

        ### print the message
        message = header+"<br>"+body
        if opts.verbose:
            print "        %s"%message
        writeLog( gracedb, opts.graceid, message, tagname=tags )

        ### apply labels
        writeLabel( gracedb, opts.graceid, set(labels) )

#---------------------------------------------------------------------------------------------------

### report all active flags
if config.getboolean("general", "allActive"):
    if opts.verbose:
        print "    allActive"

    ### set up tags
    tags  = g_tags + config.get('allActive', 'extra_tags').split()
    qtags = g_qtags + config.get('allActive', 'extra_queryTags').split()

    ### get query bounds
    start = int(gpstime-config.getfloat('allActive', 'look_left'))
    end   = gpstime+config.getfloat('allActive', 'look_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)
    dur = end-start

    gpstimeINT=int(gpstime) ### cast to int becuase the remaining query works only with ints

    ### wait until data is available
    wait = end + config.getfloat("allActive", 'wait') - lal_gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "        waiting %.3f sec"%(wait)
        time.sleep( wait )

    ### run segDB query
    outfilename = allActivefilename(start, dur, output_dir=output_dir)
    cmd = segDBallActivecmd( segdb_url, gpstimeINT, start-gpstimeINT, end-gpstimeINT, outfilename, activeOnly=False )
    if opts.verbose:
        print "        %s"%cmd
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()

    ### check return code for errors
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "        WARNING: an error occured while querying for all active flags!\n%s"%output[1]

        if not opts.skip_gracedb_upload:
            querymessage = "<b>WARNING</b>: an error occured while querying for all active flags!"
            writeLog( gracedb, opts.graceid, message=querymessage, tagname=qtags )

    ### upload to GraceDb
    elif not opts.skip_gracedb_upload:

        message = "SegDb query for all active flags within [%d, %d]"%(start, end)
        if opts.verbose:
            print "        %s"%message
        writeLog( gracedb, opts.graceid, message=message, filename=outfilename, tagname=qtags )

        ### report a human readable list
        if config.getboolean("allActive", "humanReadable"):
            file_obj = open(outfilename, "r")
            d=json.load(file_obj)
            file_obj.close()

            message = "active flags include:<br>"+", ".join(sorted(d['Active Results'].keys()))
            if opts.verbose:
                print "        %s"%message
            writeLog( gracedb, opts.graceid, message=message, tagname=tags )

#---------------------------------------------------------------------------------------------------

### report that we're done
if not opts.skip_gracedb_upload:
    message = "finished searching for segments in : %s"%(segdb_url)
    writeLog( gracedb, opts.graceid, message=message, tagname=g_tags )

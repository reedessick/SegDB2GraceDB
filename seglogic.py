#!/usr/bin/python
usage = "segLogic.py [--options] config.ini"
description = "look for segments around a GraceDB event and upload them to GraceDB"
author = "Reed Essick (reed.essick@ligo.org), Peter Shawhan (pshawhan@umd.edu)"

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

#=================================================

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

def segDBvetoDefcmd( url, vetoDef, start, end, output_dir=".", dmt=False ):
    ### ligolw_segments_from_cats_dqsegdb
    if dmt:
        return "ligolw_segments_from_cats_dqsegdb --dmt-file -v %s -s %d -e %d -i -p -o %s"%(vetoDef, start, end, output_dir)
    else:
        return "ligolw_segments_from_cats_dqsegdb -t %s -v %s -s %d -e %d -i -p -o %s"%(url, vetoDef, start, end, output_dir)

def allActivefilename( start, dur, output_dir="."):
    return "%s/allActive-%d-%d.json"%(output_dir, start, dur)

def segDBallActivecmd( url, gps, start_pad, end_pad, outfilename, activeOnly=False ):
    cmd = "ligolw_dq_query_dqsegdb -t %s -s %d -e %d -o %s %d"%(url, start_pad, end_pad, outfilename, gps)
    if activeOnly:
        cmd += " -a"
    return cmd

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

#---------------------------------------------------------------------------------------------------

### iterate through flags, uploading each to GraceDB in turn
flags = config.get( 'general', 'flags' ).split()
flags.sort( key=lambda l: config.getfloat(l,'wait')+config.getfloat(l,'look_right') ) ### sort by how soon we can launch query
for flag in flags:
    if opts.verbose:
        print "\t%s"%flag

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

    wait = end + config.getfloat(flag, 'wait') - lal_gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "\t\twaiting %.3f sec"%(wait)
        time.sleep( wait )

    outfilename = flag2filename( flag, start, dur, output_dir)
    cmd = segDBcmd( segdb_url, flag, start, end, outfilename, dmt=dmt )
    if opts.verbose:
        print "\t\t%s"%cmd
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "\tWARNING: an error occured while querying for this flag!\n%s"%output[1]
        if not opts.skip_gracedb_upload:
            message = "%s</br>&nbsp &nbsp <b>WARNING</b>: an error occured while querying for this flag!"%flag
            if config.has_option(flag, 'tagQueries'):
                queryTags = config.get(flag, 'tags').split()
            else:
                queryTags = []
            gracedb.writeLog( opts.graceid, message=message, tagname=queryTags )
        continue ### skip the rest, it doesn't make sense to process a non-existant file

    if not opts.skip_gracedb_upload:
        tags = config.get(flag, 'tags').split()
        if config.has_option(flag, 'tagQueries'):
            queryTags = tags
        else:
            queryTags = []
        message = "SegDb query for %s within [%d, %d]"%(flag, start, end)
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( opts.graceid, message=message, filename=outfilename, tagname=queryTags )

        ### process segments
        xmldoc = ligolw_utils.load_filename(outfilename, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))

        sdef = table.get_table(xmldoc, lsctables.SegmentDefTable.tableName)
        ssum = table.get_table(xmldoc, lsctables.SegmentSumTable.tableName)
        seg = table.get_table(xmldoc, lsctables.SegmentTable.tableName)

        ### get segdef_id
#        segdef_id = next(a.segment_def_id for a in sdef if a.name==flag.split(":")[1])
        segdef_id = next(a.segment_def_id for a in sdef if a.name=='RESULT')

        message = "%s"%flag

        ### define the fraction of the time this flag is defined
        ### get list of defined times
        defd = 0.0
        for a in ssum:
            if a.segment_def_id==segdef_id:
                defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns        
        message += "</br>&nbsp &nbsp defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

        ### define the fraction of the time this flag is active?
        # get list of  segments
        actv = 0.0
        flagged = 0
        for a in seg:
            if a.segment_def_id==segdef_id:
                actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                    flagged += 1
        message += "</br>&nbsp &nbsp active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")
        if flagged:
            message += "</br>&nbsp &nbsp <b>candidate is within these segments!</b>"
        else:
            message += "</br>&nbsp &nbsp <b>candidate is not within these segments!</b>"

        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( opts.graceid, message, tagname=tags )

#---------------------------------------------------------------------------------------------------

### iterate through veto definers
vetoDefiners = config.get( 'general', 'vetoDefiners' ).split()
vetoDefiners.sort( key=lambda l: config.getfloat(l,'wait')+config.getfloat(l,'look_right') ) ### sort by how soon we can launch query
for vetoDefiner in vetoDefiners:
    if opts.verbose:
        print "\t%s"%vetoDefiner

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

    ### set up GraceDB upload info
    if not opts.skip_gracedb_upload:
        tags = config.get(vetoDefiner, 'tags').split()
        if config.has_option(vetoDefiner, 'tagQueries'):
            queryTags = tags
        else:
            querytags = []
        message = "%s"%(vetoDefiner)

    wait = end + config.getfloat(vetoDefiner, 'wait') - lal_gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "\t\twaiting %.3f sec"%(wait)
        time.sleep( wait )

    ### run segDB query
    cmd = segDBvetoDefcmd( segdb_url, config.get(vetoDefiner, 'path'), start, end, output_dir=this_output_dir, dmt=dmt )
    if opts.verbose:
        print "\t\t%s"%cmd
    output = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE ).communicate()
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "\t\tWARNING: an error occured while querying for this flag!\n%s"%output[1]
        if not opts.skip_gracedb_upload:
            querymessage += "%s</br>&nbsp &nbsp &nbsp &nbsp &nbsp &nbsp<b>WARNING</b>: an error occured while querying for this flag!"%flag
            gracedb.writeLog( opts.graceid, message=querymessage, tagname=queryTags )
            message += "</br>&nbsp &nbsp %s</br>&nbsp &nbsp &nbsp WARNING: an error occured while querying for this flag!"%flag
        continue ### skip the rest, it doesn't make sense to process a non-existant file

    if not opts.skip_gracedb_upload:

        ### collect results of the query and format them into a reasonable data structure
        ifos = defaultdict( list )
        for xml in glob.glob("%s/*-VETOTIME_CAT*-%d-%d.xml"%(this_output_dir, start, dur)):
            ifos[os.path.basename(xml).split('-')[0]].append( xml )
        for ifo, val in ifos.items():
            cats = defaultdict( list )
            for xml in val:
                cats[os.path.basename(xml).split("-")[1].split("_")[-1]].append( xml )
            ifos[ifo] = cats

        ### iterate through IFOs and through Categories, extracting individual flags and summary statements
        header = "%s"%vetoDefiner
        body = ""

        for ifo in sorted(ifos.keys()):
            if opts.verbose:
                print "\t\tworking on IFO : %s"%ifo
            for category in sorted(ifos[ifo].keys()):
                if opts.verbose:
                    print "\t\t\tworking on category : %s"%category
                for xml in ifos[ifo][category]:
                    querymessage = "SegDb query for %s -> %s:%s within [%d, %d]"%(vetoDefiner, ifo, category, start, end)
                    if opts.verbose:
                        print "\t\t\t\t%s"%querymessage
                    gracedb.writeLog( opts.graceid, message=querymessage, filename=xml, tagname=queryTags )

                    if opts.verbose:
                        print "\t\t\t\treading : %s"%xml
                    xmldoc = ligolw_utils.load_filename(xml, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))
                    
                    sdef = table.get_table(xmldoc, lsctables.SegmentDefTable.tableName)
                    ssum = table.get_table(xmldoc, lsctables.SegmentSumTable.tableName)
                    seg = table.get_table(xmldoc, lsctables.SegmentTable.tableName)

                    vetoDef = table.get_table(xmldoc, lsctables.VetoDefTable.tableName)

                    ### extract info about all flags together (as a category)
                    vetoCATname = 'VETO_%s'%category
                    segdef_id = next(a.segment_def_id for a in sdef if a.name==vetoCATname)

                    header += "</br>&nbsp &nbsp %s:%s"%(ifo, category)

                    ### define the fraction of the time this flag is defined
                    ### get list of defined times
                    defd = 0.0
                    for a in ssum:
                        if a.segment_def_id==segdef_id:
                            defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                    header += "</br>&nbsp &nbsp &nbsp &nbsp defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

                    ### define the fraction of the time this flag is active?
                    # get list of  segments
                    actv = 0.0
                    flagged = 0
                    for a in seg:
                        if a.segment_def_id==segdef_id:
                            actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                            if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                                flagged += 1
                    header += "</br>&nbsp &nbsp &nbsp &nbsp active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")
                    if flagged:
                        header += "</br>&nbsp &nbsp &nbsp &nbsp <b>candidate FAILS %s:%s data quality checks</b>"%(ifo, category)
                    else:
                        header += "</br>&nbsp &nbsp &nbsp &nbsp <b>candidate PASSES %s:%s data quality checks</b>"%(ifo, category)

                    ### extract info about individual flags
                    flags = {}
                    for a in sdef: ### map flags to seg_def_id
                        if a.name!=vetoCATname:
                            flags["%s:%s:%s"%(a.ifos, a.name, a.version)] = a.segment_def_id

                    for flag in sorted(flags.keys()): ### analyze each flag individually
                        segdef_id = flags[flag]

                        body += "</br>%s (%s:%s)"%(flag, ifo, category)

                        ### define the fraction of the time this flag is defined
                        ### get list of defined times
                        defd = 0.0
                        for a in ssum:
                            if a.segment_def_id==segdef_id:
                                defd += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                        body += "</br>&nbsp &nbsp defined : %.3f/%d=%.3f%s"%(defd, dur, defd/dur * 100, "%")

                        ### define the fraction of the time this flag is active?
                        # get list of  segments
                        actv = 0.0
                        flagged = 0
                        for a in seg:
                            if a.segment_def_id==segdef_id:
                                actv += a.end_time+1e-9*a.end_time_ns - a.start_time+1e-9*a.start_time_ns
                                if (a.end_time+1e-9*a.end_time_ns >= gpstime) and (gpstime >= a.start_time+1e-9*a.start_time_ns):
                                    flagged += 1
                        body += "</br>&nbsp &nbsp active : %.3f/%d=%.3f%s"%(actv, dur, actv/dur * 100, "%")
                        if flagged:
                            body += "</br>&nbsp &nbsp <b>candidate IS within these segments</b>"
                        else:
                            body += "</br>&nbsp &nbsp <b>candidate IS NOT within these segments</b>"

        message = header+"</br>"+body
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( opts.graceid, message, tagname=tags )

#---------------------------------------------------------------------------------------------------

### report all active flags
if config.has_option("general", "allActive"):
    look_right = config.getint("allActive", "look_right")
    look_left = config.getint("allActive", "look_left")

    start = int(gpstime-config.getfloat(vetoDefiner, 'look_left'))
    end = gpstime+config.getfloat(vetoDefiner, 'look_right')
    if end%1:
        end = int(end) + 1
    else:
        end = int(end)
    dur = end-start

    gpstimeINT=int(gpstime) ### cast to int becuase the remaining query works only with ints

    wait = end + config.getfloat("allActive", 'wait') - lal_gpstime.gps_time_now() ### wait until we're past the end time
    if wait > 0:
        if opts.verbose:
            print "\t\twaiting %.3f sec"%(wait)
        time.sleep( wait )

    ### run segDB query
    outfilename = allActivefilename(start, dur, output_dir=output_dir)
    cmd = segDBallActivecmd( segdb_url, gpstimeINT, start-gpstimeINT, end-gpstimeINT, outfilename, activeOnly=False )
    if opts.verbose:
        print "\t\t%s"%cmd
    output = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE ).communicate()
    proc = sp.Popen( cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE )
    output = proc.communicate()
    if proc.returncode: ### something went wrong with the query!
        if opts.verbose:
            print "\t\tWARNING: an error occured while querying for all active flags!\n%s"%output[1]
        if not opts.skip_gracedb_upload:
            querymessage += "<b>WARNING</b>: an error occured while querying for all active flags!"
            gracedb.writeLog( opts.graceid, message=querymessage, tagname=queryTags )
 
    elif not opts.skip_gracedb_upload:
        tags = config.get("allActive","tags").split()
        if config.has_option("allActive", "tagQueries"):
            queryTags = tags
        else:
            queryTags = []

        message = "SegDb query for all active flags within [%d, %d]"%(start, end)
        if opts.verbose:
            print "\t\t%s"%message
        gracedb.writeLog( opts.graceid, message=message, filename=outfilename, tagname=queryTags )

        ### report a human readable list
        if config.has_option("allActive", "humanReadable"):
            file_obj = open(outfilename, "r")
            d=json.load(file_obj)
            file_obj.close()

            message = "active flags include:</br>"+", ".join(sorted(d['Active Results'].keys()))
            if opts.verbose:
                print "\t\t%s"%message
            gracedb.writeLog( opts.graceid, message=message, tagname=tags )

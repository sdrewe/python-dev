#!/bin/python
# -*- coding: utf-8 -*-

# $Author: stevedrewe $
# $Date: 2016-12-16 14:34:16 +0000 (Fri, 16 Dec 2016) $
# $URL: http://svn.avox.national/svn/scripts/branches/TAS-544/linux/avoxpdb1/avoxuser/python2/data_ingestion/postToEndpoint.py $
# $Rev: 3107 $


# Standard Modules
import random
import sys
import base64
import datetime
import time
import os.path
import requests
import json
import cx_Oracle
from requests.auth import HTTPBasicAuth
from multiprocessing import Process, Queue, current_process

# Custom Modules
import localEnv
import avoxUtils
from automationError import ParameterError, CountryError
import dataIngestDefn


# Set HTTP header to Send/Accept json
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def worker(taskqueue, returnqueue):
    # Entry point for worker processes
    for func, args in iter(taskqueue.get, 'STOP'):
        print "in worker", current_process().name, func.__name__
        result = func(*args)
        returnqueue.put(result, True)
        # Add small time delay to allow unix pipe to sync before next iteration
        time.sleep(0.1)


def posttoendpoint(cr_id, ep, parameters):
    # REST POST Function
    print cr_id, ep
    rs = requests.session()
    rs.headers = HEADERS
    try:
        # rs.auth = HTTPBasicAuth(localEnv.kfUser, base64.b64decode(localEnv.kfUserPwd))
        rs = requests.post(ep, headers=HEADERS, data=parameters, timeout=dataIngestDefn.httpTimeout,
                           auth=HTTPBasicAuth(localEnv.kfUser, base64.b64decode(localEnv.kfUserPwd)))
        restret = (cr_id, rs.status_code, ep, parameters)
        # time.sleep(100*random.random())
        # restret = (cr_id, 200, ep, parameters)
    except requests.Timeout, rte:
        restret = (cr_id, 504, ep, rte)
    except Exception, re_:
        restret = (cr_id, 500, ep, re_)
    finally:
        rs.close()
        return restret


def insertLog(con, rows):
    ins_csr = con.cursor()
    ins_csr.prepare(" INSERT INTO entity_process_log \
    (cr_id,event_code,status_code,event_timestamp,user_id,program_name,record_creation_ts,process_type,ovn,created_by, \
     epi_id, event_outcome, outcome_detail) \
    VALUES \
    (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10,:11,:12,:13)")
    ins_csr.executemany(None, rows)
    con.commit()
    ins_csr.close()


def updateProcessed(con, rows):
    upd_csr = con.cursor()
    # Set record to processed in the propagate table
    upd_csr.prepare(" UPDATE entity_propagate_internal \
    SET processed = 'Y', record_processed_ts = systimestamp \
    WHERE cr_id = :1 \
    AND process_type = :2 \
    AND processed = 'N'")
    upd_csr.executemany(None, rows)
    # Remove record from Data Ingestion Pot so it isn't processed again
    #upd_csr = con.cursor()
    upd_csr.prepare(" UPDATE era_queues \
    SET staff_id = (SELECT ID FROM staff WHERE username = 'SOutput') \
    WHERE cr_id = :1")
    rows = [(x[0],) for x in rows]
    upd_csr.executemany(None, rows)
    con.commit()
    upd_csr.close()


#
# Start of the main processing script
#
# Check for process lock file, if found exit
if os.path.isfile(dataIngestDefn.lockFile):
    avoxUtils.sendMail(localEnv.recipient, str(localEnv.tnsname) + "Data Ingestion skipped",
                       "Lock file found. Data Ingestion Run skipped.")
    sys.exit(0)

# Set the environment
#avoxUtils.setvars("")
starttime = datetime.datetime.now()
avoxUtils.jobctl(dataIngestDefn.pType, 'START')
# Write a lock file to cater for kofax taking longer than cron time to process the data
lckFile = open(dataIngestDefn.lockFile, 'w')
lckFile.write("Running.")
lckFile.close()
rowcount = 0
commitunit = avoxUtils.fetchArraySize()
fnTime = datetime.datetime.now().strftime("%d%m%Y%H%M%S")
excFname = 'endPointexceptions' + fnTime + '.txt'

# Get processing parameters
try:
    postBatchSize = avoxUtils.getSysParam(6495, 'kapowPOSTBatchSize', 'NUMBER')
    postProcesses = avoxUtils.getSysParam(6495, 'kapowPyProcesses', 'NUMBER')
except ParameterError as e:
    avoxUtils.jobctl(dataIngestDefn.pType, 'FAIL')
    print(e.msg)
# exit()

try:
    endPoints = {}
    jurisdictions = {}
    avoxUtils.getEndpoints(endPoints, jurisdictions)
    if len(endPoints) == 0:
        print ('No endpoints found.')
        avoxUtils.sendMail(localEnv.recipient, str(localEnv.tnsname) + ' No Endpoints found.',
                           'No Endpoints found in Application Lookups where lookup type is '
                           + dataIngestDefn.endPointLkpType)
except Exception:
    avoxUtils.jobctl(dataIngestDefn.pType, 'FAIL')
    # Raise the exception
    raise

#
# Now process the records waiting in the db
#

# Initialize the record arrays
insertArray = []
updateArray = []
excArray = []
tasklist = []
statelist = avoxUtils.getstatelist()

# Initialize the end point dictionary, this will be a dictionary of lists : {<lookup_id>:[<lists of endpoint urls>]}
epd = {}

try:
    conn = cx_Oracle.connect(localEnv.dbUser, base64.b64decode(localEnv.dbUserPwd), localEnv.tnsname)
    qry = conn.cursor()
    # evt = dataIngestDefn.dbEvt + "_API"
    
    bind = {'event': dataIngestDefn.dbEvt, 'rcount': postBatchSize}
    qry.execute(
        "SELECT * FROM (SELECT epi.cr_id, cr.status, epi.process_data, epi.epi_id \
        FROM entity_propagate_internal epi, central_records cr \
        WHERE epi.processed = 'N'  AND epi.process_type = :event AND epi.cr_id = cr.cr_id \
        ORDER BY record_creation_ts desc) \
        WHERE rownum <=:rcount", bind)
    # columns = [i[0] for i in qry.description]
    for row in qry:
        rowcount += 1
        # Get the jurisdiction value of the record
        record = row[2].read().split('^')
        rCtry = record[4]
        # We should look at only mapping state for countries with known jurisdictions at that level
        # e.g. US,CA,AU,HK via the country code state list accounting for any name variations
        prc = conn.cursor()
        twoccc = prc.var(cx_Oracle.STRING)
        states = prc.var(cx_Oracle.STRING)
        # print(rCtry)
        prc.callproc("map_country_name", [rCtry, twoccc, states])

        # Check the mappings for this value
        try:
            if twoccc.getvalue() is None:
                rJuris = None
                raise CountryError(rCtry, 'No 2-char country mapping found.')
            elif states.getvalue() == 'Y' and len(record[3]) > 0:
                rJuris = twoccc.getvalue() + '-' + record[3].upper()
            else:
                rJuris = twoccc.getvalue().upper()
            epJuris = jurisdictions[rJuris]
            # This gives the lookup_id to use to get the list of endpoints
            endpoint = endPoints[epJuris]
            # If the list of endpoints haven't been loaded then fetch them
            try:
                urls = []
                urls = epd[endpoint]
            except Exception:
                try:
                    epd[endpoint] = avoxUtils.getEPList(endpoint)
                    urls = epd[endpoint]
                except Exception:
                    raise

            # Add the process data into a dict and then dump into json
            data = {"variableName": "company_in", "attribute": [{"type": "text", "name": "legal_name_search",
                                                                 "value": record[0]}, {"type": "text", "name": "cr_id",
                                                                                       "value": str(row[0])}]}
            jData = json.dumps({"parameters": [data]})
            # Now post to each of the end point URLs
            for endpoint in urls:
                # print(endpoint)
                if endpoint.endswith("robot"):
                    try:
                        #rs = requests.post(endpoint, data = jData, timeout=dataIngestDefn.httpTimeout)
                        #rs = requests.post(endpoint, headers=headers, data=jData, timeout=dataIngestDefn.httpTimeout, auth=HTTPBasicAuth(localEnv.kfUser, base64.b64decode(localEnv.kfUserPwd)))
                        tasklist.append((posttoendpoint, (row[0], endpoint, jData)))
                        #insertArray.append((row[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                         #   dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1, 0, row[3],rs.status_code,endpoint))
                        #if rs.status_code > 299:
                        #   print (rs.status_code)
                        #   excArray.append((row[0], rJuris, "%s%s%s%s%s" % ("Received: ", str(rs.status_code), " from: ", endpoint, str(rs.content))))
                    except Exception, e:
                        # Log the error
                        excArray.append((row[0], rJuris, 'Error encountered for:' + endpoint + ' : ' + str(e)))
                        insertArray.append((row[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                                            dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1,
                                            0, row[3], 500, str(e)))
            # Add the record to the update array
            updateArray.append((row[0], dataIngestDefn.dbEvt))
            # Call the endpoints via a queue and set of workers
            # First create the task and return queues and add all the request calls to it
            task_queue = Queue()
            done_queue = Queue()
            for task in tasklist:
                task_queue.put(task, True)
            # print ('Task Queue loaded at: ', datetime.datetime.now().strftime("%d%m%Y%H%M%S"))

        except CountryError, e:
            excArray.append((row[0], e.cname, e.msg))
            updateArray.append((row[0], dataIngestDefn.dbEvt))
            insertArray.append((row[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                                dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1, 0, row[3],
                                404, e.cname + ': ' + e.msg))
        except Exception, e:
            excArray.append((row[0], rJuris, 'Endpoint Mapping failed.'))
            updateArray.append((row[0], dataIngestDefn.dbEvt))
            insertArray.append((row[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                                dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1, 0, row[3],
                                404, str(e)))

        if rowcount == commitunit:
            print('time to run a batch...')
            procs = []
            # Start worker processes
            print (postProcesses, ' Processes Starting at: ', datetime.datetime.now().strftime("%d%m%Y%H%M%S"))
            for i in range(int(postProcesses)):
                proc = Process(target=worker, args=(task_queue, done_queue))
                procs.append(proc)
                proc.start()

            # Tell worker processes to stop
            print "signal stop to workers..."
            for i in range(int(postProcesses)):
                task_queue.put('STOP', True)
                time.sleep(0.1)

            # Wait for the processes to finish and then finalize them
            print "wait for workers to stop and tidy up..."
            for p in procs:
                p.join()

            # Now dequeue all the results and add to the dml arrays
            print "start dequeue of results..."
            for i in range(len(tasklist)):
                ret = done_queue.get(True)
                time.sleep(0.1)
                for x in range(len(ret)):
                    # Add return details to the log file and the db event log array
                    # print '\t', ret[x]
                    insertArray.append((ret[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                                        dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1, 0,
                                        row[3], ret[1], ret[2]))
                    # If the HTTP code isn't in the success range add it to the exception array
                    if ret[1] > 299:
                        # print (rs.status_code)
                        excArray.append((row[0], rJuris, "%s%s%s%s%s" % ("Received: ", str(ret[1]), " from: ", ret[2],
                                                                         ret[3])))

            # Reset the tasklist list
            tasklist = []
            insertLog(conn, insertArray)
            #updateProcessed(conn, updateArray)
            # Reset the insert array
            insertArray = []
            updateArray = []
            # Write the exceptions to the file
            if len(excArray) > 0:
                excLogFile = open(excFname, 'a')
                excLogFile.write(dataIngestDefn.excLogHdr + '\n')
                for line in excArray:
                    excLogFile.write("%s%s%s%s%s%s" % (str(line[0]), "^", str(line[1]), "^", str(line[2]), "\n"))
                # Reset the exceptions array
                excArray = []

    # Check the last batch of records
    if len(insertArray) > 0:
        print "run last batch..."
        procs = []
        # Start worker processes
        print (postProcesses, ' Processes Starting at: ', datetime.datetime.now().strftime("%d%m%Y%H%M%S"))
        for i in range(int(postProcesses)):
            proc = Process(target=worker, args=(task_queue, done_queue))
            procs.append(proc)
            proc.start()

        # Tell worker processes to stop
        print "signal stop to workers..."
        for i in range(int(postProcesses)):
            task_queue.put('STOP', True)
            time.sleep(0.1)

        # Wait for the processes to finish and then finalize them
        print "wait for workers to stop and tidy up..."
        for p in procs:
            p.join()

        # Now dequeue all the results and add to the dml arrays
        print "start dequeue of results..."
        for i in range(len(tasklist)):
            ret = done_queue.get(True)
            time.sleep(0.1)
            for x in range(len(ret)):
                # Add return details to the log file and the db event log array
                # print '\t', ret[x]
                insertArray.append((ret[0], dataIngestDefn.outboundEvt, row[1], datetime.datetime.now(), 0,
                                   dataIngestDefn.progName, datetime.datetime.now(), dataIngestDefn.pType, 1, 0, row[3],
                                   ret[1], ret[2]))
                # If the HTTP code isn't in the success range add it to the exception array
                if ret[1] > 299:
                    # print (rs.status_code)
                    excArray.append((row[0], rJuris, "%s%s%s%s%s" % ("Received: ", str(ret[1]), " from: ", ret[2],
                                                                     ret[3])))

        # Reset the tasklist list
        tasklist = []
        insertLog(conn, insertArray)
        # Commented for testing
        #updateProcessed(conn, updateArray)
        # Write the exceptions to the file
        if len(excArray) > 0:
            excLogFile = open(localEnv.LOG_DIR + '/' + excFname, 'a', 0)
            excLogFile.write(dataIngestDefn.excLogHdr + '\n')
            for line in excArray:
                excLogFile.write(str(line[0]) + '^' + str(line[1]) + '^' + str(line[2]) + '\n')

except cx_Oracle.DatabaseError as e:
    error, = e.args
    avoxUtils.jobctl(dataIngestDefn.pType, 'FAIL')
    if error.code == 1017:
        print "Invalid credentials. Login failed."
    else:
        print(error)
        #print "Database connection error: %s".format(e)
        # Create log file and send email
        logFile = open(localEnv.LOG_DIR + '/processRecords.log', 'w')
        logFile.write('Database Error: ' + str(localEnv.tnsname) + '\n')
        logFile.write(str(error.message))
        logFile.close()
        avoxUtils.sendMail(localEnv.recipient, str(localEnv.tnsname) + ' Database Error.',
                           'Database Error, see log file.', localEnv.LOG_DIR + '/processRecords.log')
except Exception, e:
    print(e)
    avoxUtils.jobctl(dataIngestDefn.pType, 'FAIL')
    logFile = open(localEnv.LOG_DIR + '/processRecords.log', 'w')
    logFile.write(str(e.message))
    logFile.close()
    avoxUtils.sendMail(localEnv.recipient, str(localEnv.tnsname) + ' Python Data Ingestion Error.',
                       'Unhandled Exception, see log file.', localEnv.LOG_DIR + '/processRecords.log')
    
# Ensure that we always disconnect from the database
finally:
    qry.close()
    conn.close()
    if os.path.isfile(dataIngestDefn.lockFile):
        os.remove(dataIngestDefn.lockFile)
    if os.path.isfile(localEnv.LOG_DIR + '/' + excFname):
        excLogFile.close()
        avoxUtils.sendMail(localEnv.recipient, str(localEnv.tnsname) + ' Data Ingestion Endpoint Errors.',
                           'See attached file.', localEnv.LOG_DIR + '/' + excFname)

endtime = datetime.datetime.now()
avoxUtils.jobctl(dataIngestDefn.pType, 'SUCCESS')
print(endtime - starttime)

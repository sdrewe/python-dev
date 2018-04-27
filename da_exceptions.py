# -*- coding: utf-8 -*-
# $Author: $
# $Date: $
# $Rev: $
# $URL: $

"""
Data Attribute Exception Report: Send via email an excel spreadsheet of the new exceptions from table DA_EXCEPTION_LOG
"""
# Standard Modules
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

# 3rd Party modules
import pandas as pd

# Local Modules
from common import to_excel
from common import dss_report_class
from common.utilities import getSysParam, sendMail, localEnv
from common.usrdefexception import ParameterError, ConfigurationError
from common import oracle_conn


args = None


def connect_to_db():
    # Create db connection
    db = oracle_conn.DbConn(None)
    db.connection(localEnv.replyAddr, localEnv.recipient, localEnv.dbUser, localEnv.dbUserPwd, localEnv.tnsname,
                  localEnv.AU_BASE, localEnv.appUserId)
    db.connect()
    return db


def getbinds(connection, structure_id, this_report):
    binds = {}
    # Return a dictionary of bind variables for the structure_id in bind order with their name and source
    bind = {'strid': structure_id}
    try:
        qry = connection.conn.cursor()
        qry.execute("SELECT bind_type, bind_source_name, bind_context, bind_order FROM dss_bind_mappings \
                     WHERE structure_id = :strid \
                     ORDER BY bind_order ASC", bind)
        rows = qry.fetchmany()
        # cols = [rec[0] for rec in qry.description]
        # print rows
        for row in rows:
            # If the bind type is SYS_PARAM then get the parameter
            if row[0] == "SYS_PARAM":
                if row[1].endswith("_LR_TS"):
                    try:
                        lastrundate = getSysParam(6495, row[1], "TIMESTAMP")
                        binds["b%s" % str(row[3])] = lastrundate
                    except ParameterError as e:
                        logger.warning("Parameter Error: %s returned %s" % (row[1], e.msg.getvalue()))
                        if e.msg.getvalue() == "404":
                            lastrundate = datetime.now() - timedelta(1)
                    logger.info("Using last run date of: %s" % lastrundate)
                else:
                    binds["b%s" % str(row[3])] = getSysParam(6495, row[1], row[2])
            # If the bind type is STRUCTURE then take the value from the report instance
            elif rows[0] == "STRUCTURE":
                binds["b%s" % str(row[3])] = this_report.properties[row[1]]
            elif rows[0] == "RUN_ARG":
                binds["b%s" % str(row[3])] = getattr(args, row[1])
    finally:
        qry.close()
    return binds


if __name__ == "__main__":
    """
    
    """
    # Set the local encoding to ensure character encoding is always performed as UTF8
    os.environ["NLS_LANG"] = ".AL32UTF8"
    # Start the logger
    logger = logging.getLogger(__name__)
    # create stream handler to log to the console
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    # add the handler to the logger
    logger.addHandler(console)
    logger.setLevel(logging.INFO)

    # Start the process, get the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-type", action='store', dest='strtype')  # The structure type
    parser.add_argument("-D", action='store', dest='loglevel', default='INFO')  # The logging level to use for this run
    parser.add_argument("--version", action="version", version="%(prog)s 1.0")

    try:
        args = parser.parse_args()
        logger.info("Run options: %s" % args)
    except Exception as e:
        logger.error("Argument Error: %s" % e)
        sendMail(localEnv.recipient, str(localEnv.tnsname) + " " + args.strtype, "Input Error. {0}".format(e))
        sys.exit(1)
    # logger.info("Running report for structure: %s" % args.strtype)
    # Set the logging level
    if args.loglevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # create a file handler for the logger
    # fh = logging.FileHandler("%s%s%s%s" % (os.environ['AU_BASE'] + "/logs/", '/', os.path.splitext(sys.argv[0])[0], '.log'),
    #                          mode="w")
    # formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    # fh.setFormatter(formatter)
    # # # add the handler to the logger
    # logger.addHandler(fh)
    # logger.info("Created log file : %s", logger.handlers[1].baseFilename)
    logger.info("Starting report...")
    try:
        system = getSysParam(6495, "SYSTEM_RECIPIENT", "VARCHAR2")
    except ParameterError as e:
        logger.error("Parameter Error: %s returned %s" % ("SYSTEM_RECIPIENT", e.msg.getvalue()))
        raise

    # Get the data structure details for the report
    report = dss_report_class.Structure(args.strtype)
    logger.info("Report filename: %s" % report.filename)
    logger.info("Report recipient list: %s" % report.properties["MAIL_RECIPIENT"])

    logger.info("Building query...")
    try:
        db_connection = connect_to_db()
        try:
            query = getSysParam(int(report.source_id), report.properties["DATA_SOURCE_VIEW_NAME"], "VARCHAR2")
            if report.properties["DATA_SOURCE_VIEW_NAME"].startswith("V_"):
                logger.error("Configuration Error: Report data source cannot be a view: %s"
                             % report.properties["DATA_SOURCE_VIEW_NAME"])
                raise ConfigurationError("Report Query Build", "DATA_SOURCE_VIEW_NAME", "Must not be a view name for \
                reports")
            binds = getbinds(db_connection, report.structure_id, report)
            # Check that the bind dictionary at least matches the count of the binds in the query
            # if len(set(re.findall(r'\:b[0-9]', query))) != len(binds):
                # logger.error("Configuration Error: ")
                # sys.exit(1)
        except ParameterError as e:
            logger.error("Parameter Error: %s returned %s" % ("DA_EXC_REP_LR_TS", e.msg.getvalue()))
            raise

        # Fetch the report data into a pandas dataframe
        logger.info("Fetching data...")
        try:
            qry = db_connection.conn.cursor()
            qry.arraysize = 100
            # print query
            # print binds
            qry.execute(query, binds)
            # Convert the rows to a pandas dataframe
            df = pd.DataFrame(qry.fetchall())
            if df.empty:
                df = pd.DataFrame(columns=[d for d in report.properties["HEADER"]])
            else:
                df.columns = [d for d in report.properties["HEADER"]]
            print df

        finally:
            qry.close()

        # If any data is found then write the dataframe to an excel spreadsheet
        # and then send the spreadsheet
        logger.info("Creating spreadsheet...")
        if not df.empty:
            worksheets = {"sheet1": report.properties["STRUCTURE_DATA_TYPE"]}
            to_excel.build_xls(report.filename, worksheets, sheet1=df)
        else:
            # If no data is found then send email to notify there is nothing to report
            logger.info("No exception data returned.")
            sendMail(localEnv.recipient, str(localEnv.tnsname) + " " + args.strtype + " No Exceptions",
                     "".join("No field exception data to report since the last run."))
    finally:
        db_connection.conn.close()

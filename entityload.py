#!/bin/python
# -*- coding: utf-8 -*-
# $Author: $
# $Date: $
# $Rev: $
# $URL: $
"""
Runs data load based on the Data source structure definition in the database

It can be run from cron or command line.

Args:
    -type: The data structure type that defines the data to be loaded.
    -c: The content type of the file, F(ull) or D(elta).
    -u: The User id of the user requesting the extract.
    -f: The name of the file to load (must be a local file).
"""

# Standard Modules
import argparse
import hashlib
# import re
import os
import sys
import logging
import traceback

# Local Modules
from common import dss_class, localEnv
from common import oracle_conn
from common.utilities import sendMail, detect_by_bom
from common.usrdefexception import ConfigurationError

# 3rd Party Modules
import pandas as pd
from numpy import nan

args = None
BATCH_SIZE = 100


# Load any local overrides
try:
    import setoverrides
    localEnv.recipient = setoverrides.recipient
    localEnv.default_recipient = setoverrides.default_recipient
except ImportError as e:
    pass


def hdr_dict(row, chars):
    idx = 0
    hdridx2name = {}
    for heading in row:
        # print heading
        heading = heading.strip(chars)
        hdridx2name[heading] = idx
        idx += 1
    return hdridx2name


def insert_batch(db, rows, column_index, enc):
    """
    For each batch of records insert a row for each record Identifier into entity_file_load_gt
    and rows for each column value for each identifier into entity_file_data_gt
    Args:
        db: database connection
        rows: the table data to be loaded
        column_index: dictionary of column headers to file column index
        enc: the field enclosure character

    Returns:
    """
    # Format the complete data row into a list of tuples appropriate for the table
    efl_rows = [(row[0], row[1], row[2], row[3], row[4], None, None, row[6]) for row in rows]
    # print efl_rows
    logger.debug("Inserting entity_file_load_gt")
    try:
        inserts = db.cursor()
        inserts.prepare("INSERT INTO entity_file_load_gt (DATA_LOAD_ID, STRUCTURE_ID, FILENAME, RECORD_IDENTIFIER, \
                        DATA_HASH_VALUE, DML_IND, STATUS, CUM_RID_COUNT) \
                        VALUES (:1, :2, :3, :4, :5, :6, :7, :8)")
        inserts.executemany(None, efl_rows)
        efl_rows = []
        for row in rows:
            # print row
            for key, value in column_index.iteritems():
                record = (row[0], row[3], key, row[5][value].strip(enc))
                efl_rows.append(record)
        # print efl_rows

        logger.debug("Inserting entity_file_data_gt")
        inserts.prepare("INSERT INTO entity_file_data_gt (DATA_LOAD_ID, RECORD_IDENTIFIER, ATTRIBUTE_HEADER, \
                         ATTRIBUTE_VALUE) \
                         VALUES (:1, :2, :3, :4)")
        inserts.executemany(None, efl_rows)
    except Exception:
        db.rollback()
        raise
    finally:
        inserts.close()
        db.commit()


def convert_file(filename, enc):
    import codecs
    blocksize = 1048576

    with codecs.open(filename, "r", enc) as sourceFile:
        with codecs.open('utf8%s' % filename, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(blocksize)
                if not contents:
                    break
                targetFile.write(contents)


def read_excel(filename):
    excel = pd.ExcelFile(filename)
    df = pd.read_excel(excel, sheet_name=excel.sheet_names[0])
    df = df.fillna('')
    return df


def read_delimited(filename, sep_char):
    enc = detect_by_bom(filename, "utf-8")
    logger.info("File encoding: %s" % enc)

    if enc != 'utf-8':
        logger.info("Converting file to utf-8")
        convert_file(filename, enc)
        file2load = "utf8%s" % filename
    else:
        file2load = filename
    if sep_char == r"\t":
        logger.info("using tab delimiter...")
        df = pd.read_table(file2load, delimiter="\t", encoding="utf-8-sig")
    else:
        df = pd.read_table(file2load, delimiter=sep_char, encoding="utf-8-sig")
    # print list(df)
    df = df.replace(nan, '')
    # df = df.fillna('')
    return df


def load_file(entityload):
    # pass
    if os.path.splitext(args.filename)[1] == ".xls" or os.path.splitext(args.filename)[1] == ".xlsx":
        data = read_excel(args.filename)
    else:
        data = read_delimited(args.filename, entityload.delimiter)

    # data now contains the data from the file as a DataFrame
    # print data
    cols = list(data)
    data["C"] = data.groupby([cols[entityload.properties["CL_ID_IDX"]]]).cumcount().astype(str)
    logger.info("DataFrame memory usage (bytes): %s" % str(sys.getsizeof(data)))
    logger.info("DataFrame size (bytes): %s" % str(data.memory_usage().sum()))
    logger.info("DataFrame rows: %s" % str(len(data)))

    hdridx2name = hdr_dict(cols, entityload.enclosure)
    db = oracle_conn.DbConn(None)
    db.connection(localEnv.replyAddr, localEnv.recipient, localEnv.dbUser, localEnv.dbUserPwd, localEnv.tnsname,
                  localEnv.AU_BASE, localEnv.appUserId)
    db.connect()
    try:
        rows = []
        # get the next load id
        csr = db.conn.cursor()
        load_id = int(csr.callfunc("pkg_datapoint_load.next_load_id", oracle_conn.cx_Oracle.NUMBER))
        csr.callproc("pkg_datapoint_load.clear_tables")
        csr.close()
        load_count = 0
        row_count = 0
        # Process the data (as unicode) from the DataFrame into the staging tables
        for row in data.astype(unicode, copy=False).itertuples(index=False, name=None):
            row_count += 1
            # Map data
            # print row
            # for col in cols:
            #     print "column: %s value: %s" % (col, row[cols.index(col)])
            # Make sure everything in the row is a unicode string
            data_row = [i.encode('utf-8').decode('utf-8') for i in list(row)]
            rows.append((load_id, entityload.properties["STRUCTURE_ID"], args.filename,
                         data_row[entityload.properties["CL_ID_IDX"]].strip(entityload.properties["FIELD_ENCLOSURE"]),
                         hashlib.sha256(b"".join(data_row).encode("utf-8")).hexdigest(), tuple(data_row),
                         data_row[len(data_row)-1]))
            # print rows

            if row_count == BATCH_SIZE:
                # Load batch
                # process the records into the staging tables
                logger.debug("calling insert_batch")
                insert_batch(db.conn, rows, hdridx2name, entityload.enclosure)
                rows = []
                load_count += row_count
                row_count = 0
            # Check for and insert any last batch of rows
        if len(rows) > 0:
            insert_batch(db.conn, rows, hdridx2name, entityload.enclosure)
            load_count += row_count
        logger.info("Total file row count: %s", load_count)
    except Exception:
        raise

    # Clear the DataFrame
    del data

    # Process the records in the db staging tables
    try:
        csr = db.conn.cursor()
        outcome = csr.var(oracle_conn.cx_Oracle.STRING)
        db_load_count = csr.var(oracle_conn.cx_Oracle.NUMBER)
        db_log_id = csr.var(oracle_conn.cx_Oracle.NUMBER)
        logger.info("File loaded, Starting database processing...")
        # pkg_entity_load.load_records(p_structureType IN VARCHAR2, p_contentType IN VARCHAR2, p_userID IN NUMBER,
        #               x_loadOutcome OUT NOCOPY VARCHAR2, x_count OUT NOCOPY INTEGER, x_dss_log_id OUT NUMBER)
        csr.callproc("pkg_entity_load.load_records", [args.strtype, args.content, args.user_id, outcome,
                                                      db_load_count, db_log_id])
        logger.debug("DB OUT: outcome={0} load_count={1} log id={2}".format(outcome, db_load_count, db_log_id))
        entityload.load_count = int(db_load_count.getvalue())
        entityload.dss_log_id = db_log_id.getvalue()
        csr.close()
        logger.info("Database LOG ID: %s" % str(int(entityload.dss_log_id)))

        return outcome.getvalue()

    except oracle_conn.cx_Oracle.DatabaseError as dberror:
        subject = "{1}{0}{2}{0}{3}{0}{4}".format(" ", "ERROR:", entityload.source_name.upper(),
                                                 entityload.properties["STRUCTURE_DATA_TYPE"],
                                                 "LOAD - error")
        sendMail(list(localEnv.default_recipient), subject,
                 "Database Exception:\n {0}".format(dberror))
        logger.error("Database Exception: %s", dberror)
        return "500"


if __name__ == "__main__":
    """
    Load an entity data file into the database
    Arguments Required:
     -type the structure type for the load data
     -u the user loading the data
     -c the content type of the file
    """
    # Start the logger
    logger = logging.getLogger(__name__)
    # create stream handler to log to the console
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    # add the handler to the logger
    logger.addHandler(handler)

    # Start of the load process, get the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-type", action='store', dest='strtype')  # The structure type
    parser.add_argument("-u", action='store', dest='user_id', type=int)
    parser.add_argument("-c", action='store', dest='content', type=str)  # The file type F(ull) or D(elta)
    parser.add_argument("-f", action='store', dest='filename', type=str)
    parser.add_argument("-s", action='store', dest='src_id', type=int)
    parser.add_argument("-D", action='store', dest='loglevel', default='INFO')  # The logging level to use for this run
    parser.add_argument("--version", action="version", version="%(prog)s 1.0")

    try:
        args = parser.parse_args()
        print "Run options: ", args
    except Exception as e:
        print ("Argument Error: ", e)
        sendMail(localEnv.default_recipient, str(localEnv.tnsname) + " " + args.strtype,
                 "Input Error. {0}".format(e))
        sys.exit(1)
    # Set the logging level
    if args.loglevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # Check command line args
    try:
        try:
            uid = int(args.user_id)
        except ValueError, e:
            logger.error("-u must be an integer: %r" % args.user_id)
            sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype,
                     "Input Error. {0}".format("-u option must be an integer."))
            sys.exit(1)
        if args.strtype is None:
            logger.error("-type is required.")
            sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype,
                     "Input Error. {0}".format("-type option is required."))
            sys.exit(1)
        if args.filename is None:
            logger.error("-f is required.")
            sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype,
                     "Input Error. {0}".format("-f option is required."))
            sys.exit(1)
        if args.content is None or (args.content != 'F' and args.content != 'D'):
            logger.error("-c is required with value F or D only.")
            sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype,
                     "Input Error. {0}".format("-c option is required."))
            sys.exit(1)

    except TypeError, e:
        logger.error("Mandatory options invalid or missing.\nExiting.")
        sendMail(list(localEnv.default_recipient), "ERROR: " + sys.argv[0] + " Command Error",
                 "Command line error, mandatory options invalid or missing. \n{0}".format("Run options: " + str(args)))
        sys.exit(1)

    # Set the local encoding to ensure character encoding is always performed as UTF8
    os.environ["NLS_LANG"] = ".AL32UTF8"
    # Initialize the data structure details for the load
    try:
        try:
            entityload = dss_class.Structure(args.src_id, args.strtype, "L")
        except KeyError as e:
            raise
        # Check required os settings
        if entityload.datadir is None or entityload.log_file is None:
            sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype + " Configuration",
                     "".join(("Configuration Error running: ", args.strtype,
                              "\n\nData Directory is not set correctly.")))
            logger.error("Configuration Error running: %s, Data Directory is not set correctly.", args.strtype)
            sys.exit(1)

        # create a file handler for the logger
        fh = logging.FileHandler("%s%s%s%s" % (entityload.log_file, '/', os.path.splitext(sys.argv[0])[0], '.log'),
                                 mode="w")
        formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        fh.setFormatter(formatter)
        # add the handler to the logger
        logger.addHandler(fh)
        logger.info("Created log file : %s", logger.handlers[1].baseFilename)
        # set the log file name
        entityload.log_file = logger.handlers[1].baseFilename
        logger.info("Processing load structure: %s", args.strtype)
        logger.info("Using args: %s", args)

        # Load the data file
        load = load_file(entityload)
        # Check the return
        logger.info("Database processing returned: %s" % load)
        if load != "200":
            # Send warning email
            # Format the subject and text in the message to the recipients requirements......
            subject = "{1}{0}{2}{0}{3}{0}{4}".format(" ", "WARNING:", entityload.source_name.upper(),
                                                     entityload.properties["STRUCTURE_DATA_TYPE"],
                                                     "LOAD - incomplete")
            sendMail(list(localEnv.recipient), subject,
                     "".join(("The load has completed but returned: ", str(load), "\n",
                              str(entityload.load_count),
                              " records processed from file: ", entityload.filename,
                              "\nPlease check log ID: ", str(entityload.dss_log_id))))
        else:
            # Move the files to the DSS DATA_DIR
            logger.info("Moving files to data directory...")
            os.rename(args.filename, entityload.datadir + "/" + args.filename)
            logger.info("Completed.")

            # Send completion email
            # Format the subject and text in the message to the recipients requirements......
            subject = "{1}{0}{2}{0}{3}{0}{4}".format(" ", "SUCCESS:", entityload.source_name.upper(),
                                                     entityload.properties["STRUCTURE_DATA_TYPE"],
                                                     "LOAD - completed successfully")
            message = "The %s %s load has completed successfully." % (entityload.source_name.title(),
                                                                      entityload.properties["STRUCTURE_DATA_TYPE"])
            sendMail(list(localEnv.recipient), subject,
                     "{0}\n{1}\n\n{2}".format(message,
                                              "".join((str(entityload.load_count),
                                                       " records processed from file: ",
                                                       args.filename, "\n")),
                                              "Regards, System.")
                     )
    except ConfigurationError as e:
        logger.error("Configuration Error running {0}. Error Item (if applicable): {1} . Error raised: {2}.".format(
            e.process,
            e.item,
            e.msg))

        subject = "{1}{0}{2}{0}{3}{0}{4}".format(" ", "ERROR:", entityload.source_name.upper(),
                                                 entityload.properties["STRUCTURE_DATA_TYPE"],
                                                 "LOAD - configuration error")
        sendMail(list(localEnv.recipient), subject,
                 "Configuration Error running: {0}. Error Item (if applicable): {1}. Error raised: {2}.".format
                 (e.process, e.item, e.msg) + "\nPlease check source file content and configuration.")
    except Exception:
        logger.exception("Unhandled Exception occurred.")
        sendMail(list(localEnv.default_recipient), "ERROR: " + args.strtype,
                 "Unhandled Error running load: %s" % traceback.format_exc())

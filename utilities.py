# -*- coding: utf-8 -*-
# $Author: $
# $Date: $
# $Rev: $
# $URL: $

"""
Common Utilities module for widely used tasks, do not put single use functions in this module.
Usage: Avoid using import utilities, instead use from common.utilities import <blah>
"""
# Standard Modules
import base64
import codecs
import hashlib
import smtplib
import traceback
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename

# 3rd Party modules


# Local Modules


def fetcharraysize():
    return 100


# Return a simple MD5 HASH as a hex string
def getmd5hash(string):
    """
    :param string: input string
    :return: string: MD5 Hashed
    """
    m = hashlib.md5()
    m.update(string.encode('utf-8'))
    return m.hexdigest()


# Split, sort, join and convert to lower case a string
def sort_tokenised(inputStr):
    """ Generate split and sort lowercase"""
    inputStr = inputStr.split()
    inputStr = sorted(inputStr)
    inputStr = ''.join(inputStr)
    inputStr = inputStr.lower()
    return inputStr


def send_email(to, subject, text, filename=None):
    """ Create a text/plain message but with attachment if supplied """
    msg = MIMEMultipart()
    msg['Subject'] = subject
    fromaddr = 'noreply@nowhere.com'
    msg['From'] = fromaddr
    msg['To'] = ", ".join(to)

    part1 = MIMEText(text)
    msg.attach(part1)

    if filename is not None:
        if isinstance(filename, list):
            for f in filename:
                print (f)
                file_ = MIMEApplication(open(f).read())
                file_.add_header('Content-Disposition', 'attachment', filename=basename(f))
                msg.attach(file_)
        else:
            file_ = MIMEApplication(open(filename).read())
            file_.add_header('Content-Disposition', 'attachment', filename=basename(filename))
            msg.attach(file_)
        # file_ = MIMEApplication(open(filename).read())
        # file_.add_header('Content-Disposition', 'attachment', filename=basename(filename))
        # msg.attach(file_)

    # Send the message via our own SMTP server
    try:
        ms = smtplib.SMTP('localhost')
        ms.sendmail(fromaddr, to, msg.as_string())
        ms.quit()
    except Exception:
        return 500
    return 200


# def getSysParam(sourceID, paramName, paramType):
#     """ Get the value of a system parameter from the db """
#     try:
#         # conn = cx_Oracle.connect(localEnv.dbUser, base64.b64decode(localEnv.dbUserPwd), localEnv.tnsname)
#         """ LOCAL DEVELOPMENT ONLY """
#         dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='devpdb1.avox.national')
#         db = cx_Oracle.connect(user='avox_user', password='KeDlfMX7nwAfPurE5pA', dsn=dsn_tns)
#         print
#         db.version
#         qry = db.cursor()
#         """ END OF DEVELOPMENT """
#         # qry = conn.cursor()
#         if paramType == 'VARCHAR2':
#             pValue = qry.var(cx_Oracle.STRING)
#         elif paramType == 'NUMBER':
#             pValue = qry.var(cx_Oracle.NUMBER)
#         elif paramType == 'CLOB':
#             pValue = qry.var(cx_Oracle.CLOB)
#         elif paramType == 'TIMESTAMP':
#             pValue = qry.var(cx_Oracle.TIMESTAMP)
#
#         outcome = qry.var(cx_Oracle.STRING)
#         excID = qry.var(cx_Oracle.NUMBER)
#         qry.callproc("pkg_sys_parameter.system_parameter", [sourceID, paramName, pValue, "GET", None,
#                                                             localEnv.appUserId, outcome, excID])
#         # Check return value
#         if outcome.getvalue() != '200':
#             # mail = sendMail(localEnv.recipient, 'getSysParam Error: ' + str(outcome.getvalue()), ' for Parameter Name: '
#             #                 + paramName +' returned value: ' + str(pValue.getvalue()))
#             raise ParameterError(paramName, outcome)
#         else:
#             return pValue.getvalue()
#
#     except cx_Oracle.DatabaseError as e:
#         error, = e.args
#         if error.code == 1017:
#             print
#             "Invalid credentials. Login failed."
#         else:
#             print
#             "Database connection error: %s".format(e)
#         # Raise the exception
#         raise
#     # Ensure that we always disconnect from the database and return
#     finally:
#         qry.close()
#         # conn.close()
#         db.close()
#
#
# def setsysparam(sourceid, paramname, paramtype, paramvalue):
#     """ Set (PUT/POST) the value of a system parameter in the db """
#     try:
#         conn = cx_Oracle.connect(localEnv.dbUser, base64.b64decode(localEnv.dbUserPwd), localEnv.tnsname)
#         qry = conn.cursor()
#         if paramtype == 'VARCHAR2':
#             qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.STRING)
#         elif paramtype == 'NUMBER':
#             qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.NUMBER)
#         elif paramtype == 'TIMESTAMP':
#             qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.TIMESTAMP)
#         elif paramtype == 'CLOB':
#             qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.CLOB)
#
#         outcome = qry.var(cx_Oracle.STRING)
#         excid = qry.var(cx_Oracle.NUMBER)
#         qry.callproc("pkg_sys_parameter.system_parameter", [sourceid, paramname, paramvalue, "PUT", None,
#                                                             localEnv.appUserId, outcome, excid])
#         # Check return value
#         if outcome.getvalue() == '404':
#             # Create the parameter
#             if paramtype == 'VARCHAR2':
#                 qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.STRING)
#             elif paramtype == 'NUMBER':
#                 qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.NUMBER)
#             elif paramtype == 'TIMESTAMP':
#                 qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.TIMESTAMP)
#             elif paramtype == 'CLOB':
#                 qry.setinputsizes(cx_Oracle.NUMBER, cx_Oracle.STRING, cx_Oracle.CLOB)
#             qry.callproc("pkg_sys_parameter.system_parameter", [sourceid, paramname, paramvalue, "POST", 'REPORTING',
#                                                                 localEnv.appUserId, outcome, excid])
#             if outcome.getvalue() != '200':
#                 conn.rollback()
#                 raise ParameterError(paramname, outcome.getvalue())
#             else:
#                 conn.commit()
#         elif outcome.getvalue() != '200':
#             conn.rollback()
#             raise ParameterError(paramname, outcome.getvalue())
#         else:
#             conn.commit()
#
#     except cx_Oracle.DatabaseError as e:
#         error, = e.args
#         if error.code == 1017:
#             print
#             "Invalid credentials. Login failed."
#         else:
#             print
#             "Database connection error: {}".format(e)
#         # Raise the exception
#         raise
#     # Ensure that we always disconnect from the database and return
#     finally:
#         qry.close()
#         conn.close()
#
#
# def jobctl(name, status, step=None):
#     try:
#         conn = cx_Oracle.connect(localEnv.dbUser, base64.b64decode(localEnv.dbUserPwd), localEnv.tnsname)
#         qry = conn.cursor()
#         qry.callproc("pkg_bau_jobctl.set_status", [name, status, step])
#         conn.commit()
#     except cx_Oracle.DatabaseError as e:
#         error, = e.args
#         if error.code == 1017:
#             print
#             "Invalid credentials. Login failed."
#         else:
#             print
#             "Database error: %s".format(e)
#         # Raise the exception
#         raise
#
#     # Ensure that we always disconnect from the database
#     finally:
#         qry.close()
#         conn.close()


def detect_by_bom(path, default):
    try:
        with open(path, 'rb') as f:
            raw = f.read(4)  # will read less if the file is smaller
        for enc, boms in \
                ('utf-8-sig', (codecs.BOM_UTF8,)), \
                ('utf-16', (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)), \
                ('utf-32', (codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
            if any(raw.startswith(bom) for bom in boms):
                return enc
            else:
                # Use subprocess to get encoding using file -bi
                # file -bi sample_data.json | grep charset= | awk 'BEGIN { FS = "=" } ; {print $2}'
                try:
                    import subprocess
                    ret = subprocess.check_output(["file", "-bi", path], stderr=subprocess.STDOUT)
                    enc = ret.split("charset=", 1)[1].rstrip()
                    print("file encoding: ", enc)
                    return enc
                except subprocess.CalledProcessError as e:
                    print('CalledProcess Exception:')
                    print(e.returncode)
                    print(e.output.decode("utf-8"))
                    sendMail('steven.drewe@thomsonreuters.com', "Py 3 Test File Error",
                             "Trying to get encoding for: {0} raised: \n{1}".format(path, traceback.format_exc()))
        return default
    except IOError as ioe:
        # print "File IO error: ", ioe
        # sendMail(localEnv.recipient, str(localEnv.tnsname) + " File Error",
        #          "Trying to Read: {0} raised: \n{1}".format(path, traceback.format_exc()))
        raise


def run_bash_function(library_path, function_name, params):
    from subprocess import Popen, PIPE
    import shlex
    params = shlex.split('"source %s; %s %s"' % (library_path, function_name, params))
    cmdline = ['bash', '-c'] + params
    p = Popen(cmdline, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise RuntimeError("'%s' failed, error code: '%s', stdout: '%s', stderr: '%s'" % (
            ' '.join(cmdline), p.returncode, stdout.rstrip(), stderr.rstrip()))
    return stdout.strip()  # This is the stdout from the shell command


def format_datetime(datetime_, mask):
    """
    Only accepts standard formatting options
    :param datetime_:
    :param mask: dd, mm, yy or yyyy and hh, mi, ss and optional separator e.g. :
    :return: str (formatted datetime)
    """
    assert mask is not None
    from datetime import datetime
    if datetime_ is None or not isinstance(datetime_, datetime):
        return None
    mask = mask.replace('dd', '%d')
    mask = mask.replace('mm', '%m')
    mask = mask.replace('yyyy', '%Y')
    mask = mask.replace('yy', '%y')
    mask = mask.replace('hh', '%H')
    mask = mask.replace('mi', '%M')
    mask = mask.replace('ss', '%S')

    return datetime_.strftime(mask)


def pgp_key_file():
    from os import environ
    return environ["HOME"] + "/.gnupg/phrase"

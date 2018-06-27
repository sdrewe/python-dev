# -*- coding: utf-8 -*-
# Standard Modules
import json
import uuid
import re
import requests
import boto3
import botocore
import base64
import logging
# from itertools import islice

# Local Modules
import neptune


def upload_file(file_):
    session = boto3.Session(
        aws_access_key_id=base64.b64decode(neptune.ACCESS_KEY_ID),
        aws_secret_access_key=base64.b64decode(neptune.ACCESS_SECRET_KEY),
        region_name=neptune.S3REGION
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(neptune.BUCKET_NAME)
    try:
        s3.meta.client.head_bucket(Bucket=neptune.BUCKET_NAME)
        bucket.upload_file(file_, file_)
        logger.info("Uploaded file: %s to bucket: %s" % (file_, neptune.BUCKET_NAME))
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logger.error("Bucket %s not found." % neptune.BUCKET_NAME)
        elif error_code == 403:
            logger.error("Bucket name invalid: %s" % neptune.BUCKET_NAME)


def post2neptune(file_):
    # pass
    jdata = {"source": "s3://bucket-name/%s" % file_,
             "format": "format",
             "iamRoleArn": "arn:aws:iam::{0}:role/{1}".format(neptune.ACID, neptune.IAM_ROLE),
             "region": neptune.REGION,
             "failOnError": "FALSE"}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    rs = requests.session()
    rs.headers = headers
    try:
        # pass
        rs = requests.post(neptune.ENDPOINT + " -d ", headers=headers, data=json.dumps(jdata, ensure_ascii=False))
        # Get the status of the request and if ok then get the status of the load
        print rs.status_code
        print rs.content
        loadid = json.loads(rs.content)["payload"]["loadId"]
        print loadid
        if rs.status_code == 200:
            rs = requests.get(neptune.ENDPOINT + "/" + loadid)
            print "loadid GET: ", rs.status_code
            print "load status: ", json.loads(rs.content)["payload"]["overallStatus"]["status"]
    except Exception:
        raise
    finally:
        rs.close()


def build_file_hdr(mapping):
    data_row = []
    for field in mapping:
        # print field, mapping[field]
        try:
            col = neptune.NEPTUNE[field]
            data_row.append(col)
        except KeyError:
            data_row.append(field)
    print "HEADER: %s" % neptune.SEPARATOR.join(data_row)
    return neptune.SEPARATOR.join(data_row)


def extract_from_json(mapping, data):
    # Read the data from the file row
    for field in mapping:
        element = mapping[field]
        print element
        if element == '$UUID$':
            data_row.append('"%s"' % str(uuid.uuid4()))
        elif element.isupper() and element.startswith("#"):
            data_row.append('"%s"' % element.lstrip("#"))
        elif element.startswith("\\"):
            # print ('"%s"' % data[element.lstrip("\\")])
            data_row.append('"%s"' % data[element.lstrip("\\")])
        elif field.endswith("[]"):
            # Array type that must be converted to a MULTISET of strings
            # print '"%s"' % ';'.join([(str(x)) for x in data[root][element]])
            data_row.append('"%s"' % ';'.join([(str(x)) for x in data[root][element]]))
        else:
            # print str(data[root][element])
            data_row.append('"%s"' % data[root][element])
    return data_row


logger = logging.getLogger()
logger.setLevel(logging.INFO)
# create stream handler to log to the console
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-6s %(levelname)-6s %(message)s")
handler.setFormatter(formatter)
# add the handler to the logger
logger.addHandler(handler)

data_row = []
filename = 'BasicCompanyData-test.csv'
# filename = 'psc-snapshot-test.csv'
logger.info("Processing: %s", filename)
# Load the data file definitions
with open('filedefs.json') as json_data:
    definition = json.load(json_data)

# Process the data file
if filename.startswith("BasicCompanyData"):
    file_type = "BasicCompanyData"
    mapping = definition[file_type]["vertex"]["mapping"]
    file_header = definition[file_type]["header"]
    # print file_header
    # print mapping
    # Build the header record for the output file
    hdr = build_file_hdr(mapping)
    with open("company_vertex.csv", 'w') as outputfile:
        outputfile.write(hdr + "\n")

        with open(filename, mode="r") as datafile:
            # Skip Header
            next(datafile)
            # for line in islice(datafile, 12, 13):
            for line in datafile:
                # print line
                del data_row[:]
                # data_list = [x.strip('" ') for x in line.split(",")]
                pattern = r'\{0}(?=(?:[^\{1}]*\{1}[^\{1}]*\{1})*[^\{1}]*$)'.format(definition[file_type]["separator"],
                                                                                   definition[file_type]["enclosure"])
                data_list = re.split(pattern, line.rstrip())
                data_list = [x.strip(' ') for x in data_list]
                # print data_list
                for field in mapping:
                    # print field, mapping[field]
                    # print file_header.index(str(mapping[field]))
                    element = mapping[field]
                    # print element
                    if element.isupper() and element.startswith("#"):
                        data_row.append('"%s"' % element.lstrip("#"))
                    else:
                        data_row.append('"%s"' % data_list[file_header.index(str(mapping[field]))])
                outputfile.write(neptune.SEPARATOR.join(data_row) + "\n")


elif filename.startswith("psc-snapshot"):
    """
    To create the relationships there needs to be a node for each person entity created via node file and then
    the vertex file to map the person to the company
    """
    file_type = "psc-snapshot"
    file_format = definition[file_type]["type"]
    root = definition[file_type]["root"]

    if file_format == "json":
        for key in definition[file_type].keys():
            print key, type(definition[file_type][key]).__name__
    # print type(definition[file_type]["addresses"]["RegAddress"])

    # Process the vertices
    mapping = definition[file_type]["vertex"]["mapping"]
    # Build the header record for the output file
    hdr = build_file_hdr(mapping)
    with open("vertex.csv", 'w') as outputfile:
        outputfile.write(hdr + "\n")

        with open(filename, mode="r") as datafile:
            # first_line = datafile.readline()
            # Skip Header
            next(datafile)
            for line in datafile:
                del data_row[:]
                data = json.loads(line)
                # Read the data from the file row
                for field in mapping:
                    element = mapping[field]
                    # print element
                    if element.isupper() and element.startswith("#"):
                        data_row.append('"%s"' % element.lstrip("#"))
                    elif element.startswith("\\"):
                        # print data[element.lstrip("\\")]
                        data_row.append('"%s"' % data[element.lstrip("\\")])
                    else:
                        # print data[root][mapping[field]]
                        try:
                            data_row.append('"%s"' % data[root][element])
                        except KeyError:
                            data_row.append('""')
                outputfile.write(neptune.SEPARATOR.join(data_row) + "\n")

    # Process the edges
    mapping = definition[file_type]["edge"]["mapping"]
    # Build the header record for the output file
    hdr = build_file_hdr(mapping)
    with open("peco_edge.csv", 'w') as outputfile:
        outputfile.write(hdr + "\n")
        data_row = []
        output_row = []

        with open(filename, mode="r") as datafile:
            # first_line = datafile.readline()
            # for line in islice(datafile, 2, 3):
            #     first_line = line
            for line in datafile:
                del data_row[:]
                del output_row[:]
                data = json.loads(line)
                data_row = extract_from_json(mapping, data)
                # print data_row

                # Read the data from the file row
                for field in mapping:
                    element = mapping[field]
                    # print element
                    if element == '$UUID$':
                        output_row.append('"%s"' % str(uuid.uuid4()))
                    elif element.isupper() and element.startswith("#"):
                        output_row.append('"%s"' % element.lstrip("#"))
                    elif element.startswith("\\"):
                        # print ('"%s"' % data[element.lstrip("\\")])
                        output_row.append('"%s"' % data[element.lstrip("\\")])
                    elif field.endswith("[]"):
                        # Array type that must be converted to a MULTISET of strings
                        # print '"%s"' % ';'.join([(str(x)) for x in data[root][element]])
                        output_row.append('"%s"' % ';'.join([(str(x)) for x in data[root][element]]))
                    else:
                        # print str(data[root][element])
                        output_row.append('"%s"' % data[root][element])

                outputfile.write(neptune.SEPARATOR.join(output_row) + "\n")

        # Send the generated file to s3 bucket
        upload_file(file_="peco_edge.csv")
        # Load the file contents into Neptune
        # post2neptune(filename)

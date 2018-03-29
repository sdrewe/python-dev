# -*- coding: utf-8 -*-
import json
import uuid
import re
from itertools import islice

import neptune


def build_file_hdr(mapping):
    data_row = []
    # print "BUILDING HEADER:"
    for field in mapping:
        # print field, mapping[field]
        try:
            col = neptune.neptune[field]
            print col
            data_row.append(col)
        except KeyError:
            data_row.append(field)
            pass
    print "HEADER: %s" % neptune.SEPARATOR.join(data_row)
    return neptune.SEPARATOR.join(data_row)


data_row = []
filename = 'BasicCompanyData-test.csv'
# filename = 'psc-snapshot-test.csv'

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
            # for line in islice(datafile, 12, 13):
            for line in datafile:
                # print line
                del data_row[:]
                # data_list = [x.strip('" ') for x in line.split(",")]
                pattern = r'\{0}(?=(?:[^\{1}]*\{1}[^\{1}]*\{1})*[^\{1}]*$)'.format(definition[file_type]["separator"],
                                                                                   definition[file_type]["enclosure"])
                data_list = re.split(pattern, line.rstrip())
                data_list = [x.strip(' ') for x in data_list]
                print data_list
                for field in mapping:
                    # print field, mapping[field]
                    # print file_header.index(str(mapping[field]))
                    element = mapping[field]
                    print element
                    if element.isupper() and element.startswith("#"):
                        data_row.append('"%s"' % element.lstrip("#"))
                    else:
                        data_row.append('"%s"' % data_list[file_header.index(str(mapping[field]))])
                outputfile.write(neptune.SEPARATOR.join(data_row) + "\n")


elif filename.startswith("psc-snapshot"):
    """
    To create the relationships there needs to be a node for each person entity created via node file and then
    the vertex file
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
            first_line = datafile.readline()
            data = json.loads(first_line)
            # Read the data from the file row
            for field in mapping:
                element = mapping[field]
                if element.isupper() and element.startswith("#"):
                    data_row.append('"%s"' % element.lstrip("#"))
                elif element.startswith("\\"):
                    # print data[element.lstrip("\\")]
                    data_row.append('"%s"' % data[element.lstrip("\\")])
                else:
                    # print data[root][mapping[field]]
                    data_row.append('"%s"' % data[root][mapping[field]])

            outputfile.write(neptune.SEPARATOR.join(data_row) + "\n")

    # Process the edges
    mapping = definition[file_type]["edge"]["mapping"]
    # Build the header record for the output file
    hdr = build_file_hdr(mapping)
    with open("peco_edge.csv", 'w') as outputfile:
        outputfile.write(hdr + "\n")
        data_row = []

        with open(filename, mode="r") as datafile:
            # first_line = datafile.readline()
            for line in islice(datafile, 2, 3):
                print line
                first_line = line
            data = json.loads(first_line)

            # Read the data from the file row
            print "EDGE MAPPING LOOP:"
            print data
            for field in mapping:
                pass
                element = mapping[field]
                print element
                if element == '$UUID$':
                    print str(uuid.uuid4())
                    data_row.append('"%s"' % str(uuid.uuid4()))
                elif element.isupper():
                    print element
                    data_row.append('"%s"' % element)
                elif element.startswith("\\"):
                    print ('"%s"' % data[element.lstrip("\\")])
                    data_row.append('"%s"' % data[element.lstrip("\\")])
                elif field.endswith("[]"):
                    # Array type that must be converted to a MULTISET of strings
                    print '"%s"' % ';'.join([(str(x)) for x in data[root][element]])
                    data_row.append('"%s"' % ';'.join([(str(x)) for x in data[root][element]]))
                else:
                    print str(data[root][element])
                    data_row.append('"%s"' % data[root][element])

            outputfile.write(neptune.SEPARATOR.join(data_row) + "\n")

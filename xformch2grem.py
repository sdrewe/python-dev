# -*- coding: utf-8 -*-

# Standard Modules
import argparse
import logging
import io
import sys

# Local Modules
import neptune
import datafiledefinition

args = None

def hdr_dict(row, chars):
    idx = 0
    hdridx2name = {}
    for heading in row:
        # print heading
        heading = heading.strip(chars)
        hdridx2name[heading] = idx
        idx += 1
    return hdridx2name


def main(filename):
    """

    :return:
    """
    global args
    # Start the logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Start of the load process, get the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", action='store', dest='filename', type=str)
    try:
        args = parser.parse_args()
        logger.info("Run options: ", args)
    except Exception as e:
        logger.error("Argument Error: ", e)
        sys.exit(1)
    # Call the transformer
    # Use the file name to determine the type of file to output, Vertex or Edge
    datafile = datafiledefinition.Structure(args.filename)
    # Read the data file
    try:
        with io.open(filename, encoding="utf-8", mode="r") as loadfile:
            logger.debug("opened %s file" % filename)
            for row in loadfile:
                # Cater for empty rows
                if not row.rstrip():
                    continue
                # If file is json then there is no header record
                if datafile.isjson == "N":
                    # Build the header dictionary of the actual file columns
                    hdridx2name = hdr_dict(row, datafile.enclosure)
                    logger.info(hdridx2name)


    except IOError as ioe:
        logger.error("File IO error: %s", ioe)
        raise
    pass


if __name__ == "__main__":
    main(filename=args.filename)

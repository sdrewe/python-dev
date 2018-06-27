# -*- coding: utf-8 -*-
# $Author: $
# $Date: $
# $Rev: $
# $URL: $

import cx_Oracle
import json
import entitymodel
import datetime


def entity_to_json(entity):
    if type(entity) != entitymodel.Entity:
        raise TypeError("Expected entitymodel.Entity got %s" % type(entity))
    # Convert the entity object and datapoints to json
    pass


if __name__ == "__main__":
    # dpdef = {"name": "DATAPOINTNAME", "value": "DATAPOINTVALUE"}
    # Create db connection
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='tba')
    conn = cx_Oracle.connect(user='user', password='', dsn=dsn_tns)
    print conn.version
    typeObj = conn.gettype("DATAPOINT_OBJ")
    print typeObj.attributes
    qry = conn.cursor()
    qry.execute("select avid, content_timestamp, legal_name, previous_names \
                from v_6495_avid_full_v2 where cr_id = 24951545")
    print qry.description
    # for row in qry:
    #     print row
    #     # print row[0].(dpdef["name"])#, row[0].dpdef["value"]
    #     print getattr(row[0], dpdef["name"]), getattr(row[0], dpdef["value"])
    #     entity_dict = {getattr(row[0], dpdef["name"]): getattr(row[0], dpdef["value"]), "CONTENT TIMESTAMP": row[1]}
    #     print json.dumps(entity_dict)

    for row in qry:
        for idx, val in enumerate(row):
            print val
            print "Cursor Type: ", qry.description[idx][1]
            if type(val) == cx_Oracle.Object:
                print "cx type: ", val.type.attributes
                if val.type.iscollection:
                    ix = val.first()
                    while ix is not None:
                        # print(ix, "->", val.getelement(ix))
                        for attr in val.getelement(ix).type.attributes:
                            print attr.name, getattr(val.getelement(ix), attr.name)
                        ix = val.next(ix)
                else:
                    for attr in val.type.attributes:
                        print attr.name, getattr(val, attr.name)
            else:
                print qry.description[idx][0], val



    qry.close()
    conn.close()

    # eoi = entitymodel.Entity("25405760", "6495", "ACTIVE", datetime.datetime.now())
    # dp

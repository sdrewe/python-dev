# -*- coding: utf-8 -*-
"""
Generate a pdf or excel report of the Master Attributes List with database sourced data types and modules
"""
# Standard Modules
import sys

# 3rd Party Modules
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import inch
from reportlab.platypus import (
    BaseDocTemplate,
    PageTemplate,
    Frame,
    Paragraph,
    Image,
    PageBreak,
    ListFlowable
)
from reportlab.lib.styles import ParagraphStyle, ListStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.colors import (
    black,
    gainsboro
)
from reportlab.platypus import Table, TableStyle
# import cx_Oracle
import argparse

# Local Modules
from numberedcanvas import NumberedCanvas
import oracle_conn
import localEnv


global args


def get_data(filetype):
    try:
        # Create db connection
        db = oracle_conn.orcle_conn("")
        db.connection(localEnv.replyAddr, localEnv.recipient, localEnv.dbUser, localEnv.dbUserPwd, localEnv.tnsname,
                      localEnv.AU_BASE, localEnv.appUserId)
        db.connect()
        # print db.version
        qry = db.conn.cursor()
        qry.execute("SELECT sysdate FROM dual")
        # ts = db.conn.cx_Oracle.DATETIME
        # ts = datetime.datetime.now()
        for row in qry:
            ts = row[0]
            print ts
        data = []
        # Set the context to be the Version for the report
        qry.callproc("pkg_context_api.data_channel_set_param", ['api_version', '1'])
        qry.execute(" SELECT \"Identifier\", \"Name\", \"Module\", \"Data Type\", \"Maximum Length\", \
                    \"Data Point Type\", \"XML Element or Attribute Name\" FROM V_6495_EXTERNAL_ATTRIBUTES \
                    ORDER BY \"Module\", \"Identifier\" ")

        if filetype == "pdf":
            print "Building pdf data"
            data.append([i[0] for i in qry.description])
            for row in qry:
                # datarow = [row[0], Paragraph(row[1], style), row[2], row[3], row[4], row[5], row[6], row[7]]
                data.append([Paragraph("TBC", stylesheet['tabletext']) if col is None else
                             Paragraph(col, stylesheet['tabletext']) for col in row])
        elif filetype == "xls":
            print "Building xls data"
            columns = [i[0] for i in qry.description]
            for row in qry:
                data.append([col for col in row])

        qry.callproc("pkg_context_api.data_channel_clear_all_context")
        # qry.close()

    except Exception:
        raise
    finally:
        qry.close()
        db.disconnect()
    return data, columns


REP_NOTES = "Table Notes:<br/>"


def stylesheet():
    styles = {
        'default': ParagraphStyle(
            'default',
            fontName='Times-Roman',
            fontSize=8,
            leading=12,
            leftIndent=0,
            rightIndent=0,
            firstLineIndent=0,
            alignment=TA_LEFT,
            spaceBefore=0,
            spaceAfter=0,
            bulletFontName='Times-Roman',
            bulletFontSize=2,
            bulletIndent=0,
            textColor=black,
            backColor=None,
            wordWrap='LTR',
            borderWidth=0,
            borderPadding=0,
            borderColor=None,
            borderRadius=None,
            allowWidows=1,
            allowOrphans=0,
            textTransform=None,  # 'uppercase' | 'lowercase' | None
            endDots=None,
            splitLongWords=1,
        ),
    }
    styles['title'] = ParagraphStyle(
        'title',
        parent=styles['default'],
        fontName='Courier-Oblique',
        fontSize=20,
        leading=42,
        alignment=TA_CENTER,
        textColor=black,
        spaceBefore=70,
        spaceAfter=70,
    )
    styles['subheading'] = ParagraphStyle(
        'subheading',
        parent=styles['default'],
        fontName='Helvetica',
        fontSize=14,
        leading=42,
        alignment=TA_LEFT,
        textColor=black,
    )
    styles['alert'] = ParagraphStyle(
        'alert',
        parent=styles['default'],
        leading=14,
        backColor=gainsboro,
        borderColor=black,
        borderWidth=1,
        borderPadding=5,
        borderRadius=2,
        spaceBefore=20,
        spaceAfter=20,
    )
    styles['footer'] = ParagraphStyle(
        'footer',
        parent=styles['default'],
        leading=14,
        backColor=gainsboro,
        borderColor=black,
        borderWidth=1,
        borderPadding=5,
        borderRadius=2,
        spaceBefore=50,
        spaceAfter=0,
        alignment=TA_CENTER,
    )
    styles['spacer'] = ParagraphStyle(
        'spacer',
        parent=styles['default'],
        spaceBefore=25,
        spaceAfter=25,
        alignment=TA_CENTER,
    )
    styles['tabletext'] = ParagraphStyle(
        'tabletext',
        parent=styles['default'],
        spaceBefore=0,
        spaceAfter=0,
        alignment=TA_CENTER,
        wordwrap='LTR',
        fontName='Helvetica',
        fontSize=5,
    )
    styles['table'] = TableStyle(
        [
         ('ALIGN', (0,0), (-1,-1), 'CENTER'),
         ('FONT', (0,0), (-1,0), 'Helvetica-Bold'),
         ('FONTSIZE', (0, 0), (-1, 0), 8),
         ('TEXTCOLOR', (1,1), (-1,-1), black),
         # ('VALIGN', (0,0), (0,-1), 'TOP'),
         ('INNERGRID', (0,0), (-1,-1), 0.25, black),
         ('BOX', (0,0), (-1,-1), 0.5, black),
         ('FONTSIZE', (0,0), (-1,-1), 5),
         ('LEFTPADDING', (0, 0), (-1, -1), 2),
         ('RIGHTPADDING', (0, 0), (-1, -1), 2)
        ]
    )
    styles['list_default'] = ListStyle(
        'list_default',
        leftIndent=18,
        rightIndent=0,
        spaceBefore=0,
        spaceAfter=0,
        bulletAlign='left',
        # bulletType='bullet',
        bulletColor=black,
        # bulletFontName='Helvetica',
        bulletFontSize=8,
        bulletOffsetY=0,
        bulletDedent='auto',
        bulletDir='ltr',
    )
    return styles


def build_pdf(filename, content):
    doc = BaseDocTemplate(filename, showBoundary=0)
    # doc.watermark = 'Draft'
    doc.addPageTemplates(
        [
            PageTemplate(
                frames=[
                    Frame(
                        doc.leftMargin,
                        doc.bottomMargin,
                        doc.width,
                        doc.height,
                        id=None
                    ),
                ]
            ),
        ]
    )
    doc.build(content, canvasmaker=NumberedCanvas)


def build_content(stylesheet):

    data = get_data("pdf")
    # Add header image
    return [
        Image("TRLogo.jpg", width=5*inch, height=3*inch, kind='proportional'),
        Paragraph("Managed Data Service<br/>Data Point Definitions", stylesheet['title']),
        Paragraph("Release Version: %s%s%s%s" % ("2017.8", "<br/>", "Content Timestamp: ", str(ts)),
                  stylesheet['alert']),
        PageBreak(),
        Paragraph(" ", stylesheet['spacer']),
        Paragraph("MDS Data Points", stylesheet['subheading']),
        Paragraph(REP_NOTES, stylesheet['default']),
        ListFlowable(
            [
                Paragraph('Identifier: The unique identifier for the field.', stylesheet['default']),
                Paragraph('Module: Grouping of fields, "CLIENT DATA" represents data that can be sent to MDS.',
                          stylesheet['default']),
                Paragraph('V1 Only: If Yes, then the field is not available in Version 2.', stylesheet['default']),
                Paragraph('XML Parent or Attribute Name: If all uppercase, then this is the '
                          '//EntityAttributes/EntityAttribute/AttributeName text, '
                          'otherwise it is the Parent Element Name.', stylesheet['default']),
                Paragraph('TBC: To Be Confirmed.', stylesheet['default']),
            ],
            bulletType='bullet',
            bulletFontSize=4,
            leftIndent=10,
            bulletOffsetY=-2,
            start='circle'
        ),
        Paragraph(" ", stylesheet['spacer']),
        Table(data, style=stylesheet['table'], repeatRows=1),
        Paragraph("End of Document", stylesheet['footer']),
    ]


def build_xls(filename):
    import pandas
    print filename
    data = get_data("xls")
    cols = data[1]
    print cols
    rows = data[0]
    df = pandas.DataFrame.from_records(rows, columns=cols)
    # df.drop(df.columns[[0]], axis=1, inplace=True)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pandas.ExcelWriter(filename, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object removing the index column at the front.
    df.to_excel(writer, sheet_name='Database Field List', index=False, freeze_panes=(1, 0))

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-type", action='store', dest='doctype')  # The output document type
    parser.add_argument("-f", action='store', dest='filename')  # The output filename, optional override

    try:
        global args
        args = parser.parse_args()
    except Exception as e:
        print ("Argument Error: ", e)
        sys.exit(1)
    # print args.strtype
    # Check command line args
    if args.doctype is None:
        print ("-type is required.")
        sys.exit(1)
    elif args.doctype == "xls":
        try:
            if args.filename is not None:
                build_xls(args.filename + ".xlsx")
            else:
                build_xls("MDSDataPoints.xlsx")
        except Exception:
            raise
    elif args.doctype == "pdf":
        try:
            if args.filename is not None:
                build_pdf(args.filename + ".pdf", build_content(stylesheet()))
            else:
                build_pdf('MDSDataPoints.pdf', build_content(stylesheet()))
        except Exception:
            raise
    else:
        print "Unsupported output type: ", args.doctype

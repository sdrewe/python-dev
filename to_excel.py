# -*- coding: utf-8 -*-
"""
Generate an excel workbook of the data passed. All files have extension .xlsx
All input data must be a pandas.DataFrame
"""

# Standard Modules

# 3rd Party Modules
import pandas as pd

# Local Modules


def build_xls(filename, worksheets, **kwargs):
    """
    Args:
        filename: The name of the Excel workbook to be created
        worksheets: Dictionary of worksheet names, if not provided the default Excel names are output
        **kwargs: The data sheets and data to be written to the spreadsheet in format sheet1={DataFrame}
        , sheet2={DataFrame}

    Returns:
    """
    print filename
    print len(worksheets)

    if kwargs is None:
        print "Worksheet Data is required."
        return
    else:
        # Create the workbook
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(filename, engine='xlsxwriter', datetime_format='yyyy-mm-dd hh:mm:ss',
                                date_format='yyyy-mm-dd')
        workbook = writer.book
        header_format = workbook.add_format({'bold': True,
                                             'align': 'center',
                                             'valign': 'vcenter',
                                             'fg_color': '#D7E4BC',
                                             'border': 1})
        # For each worksheet create the sheet and write the data
        sheet_no = 0
        for key in kwargs:
            sheet_no += 1
            # Convert the dataframe to an XlsxWriter Excel object removing the index column at the front.
            try:
                sheet_name = worksheets[key]
                kwargs[key].to_excel(writer, sheet_name=sheet_name, index=False, freeze_panes=(1, 0))
            except KeyError:
                sheet_name = "Sheet%s" % sheet_no
                kwargs[key].to_excel(writer, sheet_name=sheet_name, index=False, freeze_panes=(1, 0))
            # Apply the formatting
            worksheet = writer.sheets[sheet_name]
            for i in range(len(kwargs[key].columns)):
                xcol = chr(ord("A")+i)
                # print xcol
                worksheet.set_column("{0}:{0}".format(xcol),
                                     max(kwargs[key][list(kwargs[key])[i]].apply(str).map(len).max(),
                                         len(list(kwargs[key])[i]))+2)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

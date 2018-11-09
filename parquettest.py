# -*- coding: utf-8 -*-
import pandas as pd
import pyarrow as pa

# df = pd.DataFrame()
# Sample Data
raw_data = {'first_name': ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'],
        'last_name': ['Miller', 'Jacobson', 'Ali', 'Milner', 'Cooze'],
        'age': [42, 52, 36, 24, 73],
        'preTestScore': [4, 24, 31, 2, 3],
        'postTestScore': [25, 94, 57, 62, 70]}
# Create Dataframe for the Sample Data
df = pd.DataFrame(raw_data, columns = ['first_name', 'last_name', 'age', 'preTestScore', 'postTestScore'])

# Write the Dataframe to parquet file
df.to_parquet("ptest.parquet", engine='auto', compression='snappy')
# df.to_csv("ptestcsv.csv")

# Now read back the parquet file and print the contents
data = pa.parquet.read_table('ptest.parquet')
# print (data)
df2 = data.to_pandas()
print (df2)

# Read selected columns into a dataframe
dfnames = pa.parquet.read_pandas('ptest.parquet', columns=['first_name', 'last_name']).to_pandas()
# print (dfnames.sort_values('last_name', ascending=True))
dfnames.sort_values('last_name', ascending=True, inplace=True)
print (dfnames)

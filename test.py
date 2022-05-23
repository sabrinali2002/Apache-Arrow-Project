import sys
from pyarrow import csv
import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
import pandas as pd
import pyarrow.compute 

fn = 'bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv'
table = csv.read_csv(fn)
# table = np.array(csv.read_csv(fn))
#feather.write_feather(df, file_path)
#switch_on = input("Turn off dictionary encoding for a column? (Y/N) : ")
#if switch_on == "Y" or switch_on == "yes" or switch_on == "Yes":
#    column = input("Enter a number: ")
#    table = np.delete(table,int(column),0)
chunks = []
encoded_chunks = []
name_list = []
for column in table:
	str_column = column.cast('string')
	encoded_chunks.append(str_column.dictionary_encode().combine_chunks())
	chunks.append(str_column.combine_chunks())
for chunk in encoded_chunks:
	name_list.append("");
encoded_table = pa.Table.from_arrays(encoded_chunks, name_list)
nonencoded_table = pa.Table.from_arrays(chunks, name_list)

print("encoded and uncompressed: " + str(sys.getsizeof(encoded_table)))
print("nonencoded and uncompressed: " + str(sys.getsizeof(nonencoded_table)))

feather.write_feather(encoded_table, "./encoded_compressed", compression='zstd')
feather.write_feather(nonencoded_table, "./nonencoded_compressed", compression='zstd')
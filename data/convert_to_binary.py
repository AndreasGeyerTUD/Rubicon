import pandas as pd
import re
import numpy as np
import concurrent.futures
from pathlib import Path
import argparse
from progressbar import progressbar

dsp_int64 = np.dtype('<i8')
dsp_bool = np.dtype('<i1')

def serialize_int(fname, column):
    with open(fname, 'wb') as out:
        out.write(column.to_numpy(dtype=dsp_int64).tobytes())

def serialize_bool(fname, column):
    with open(fname, 'wb') as out:
        out.write(column.to_numpy(dtype=dsp_bool).tobytes())

def serialize_date(fname, column):
    with open(fname, 'wb') as out:
        dates = column.to_numpy(dtype=dsp_int64)
        years = dates // 10000
        months = (dates % 10000) // 100
        days = dates % 100
        dates_dt64 = np.array([np.datetime64(f'{year}-{month:02d}-{day:02d}', 'D') for year, month, day in zip(years, months, days)])
        out.write((dates_dt64 - np.datetime64('1970-01-01', 's')).astype(dsp_int64).tobytes())

def serialize_str(fname, column):
    with open(fname, 'wb') as out:
        data = column.to_numpy(dtype=np.str_)
        unique_strings = sorted(set(np.unique(data)) - {"NULL"})
        unique_strings = ["NULL"] + unique_strings  # NULL always at index 0
        
        string_to_int = {s: i for i, s in enumerate(unique_strings)}
        encoded_data = np.vectorize(lambda x: string_to_int[x])(data)
        
        with open(fname.replace(".bin", "_dict.tsv"), 'w') as dict_out:
            dict_out.writelines(f"{key}\t{value}\n" for key, value in string_to_int.items())

        out.write(encoded_data.astype(dsp_int64).tobytes())

SERIALIZERS = {
    "int": serialize_int,
    "bool": serialize_bool,
    "date": serialize_date,
    "str": serialize_str
}

def serialize_and_write(column_name, dtype_as_str, column_data, output_dir):
    serializer = SERIALIZERS.get(dtype_as_str, serialize_int)  # Default to int
    serializer(f"{output_dir}/{column_name}.bin", column_data)


def parallel_serialize(table_name, df, output_dir, schema):
    out = f"{output_dir}/{table_name}"
    Path(out).mkdir(parents=True, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(serialize_and_write, schema[table_name][col][0], schema[table_name][col][1], df[col], out): col for col in range(len(schema[table_name]))
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Catch any exceptions
            except Exception as e:
                print(f"Error serializing {futures[future]}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("convert_to_binary")
    parser.add_argument("--input", help="Where to find the input (the csv files).", type=str)
    parser.add_argument("--output", help="Where to put the output (the binary files).", type=str)
    parser.add_argument("--schema", help="Where to find the schema file (default=./schema.txt).", default="schema.txt", type=str)
    args = parser.parse_args()

    schema = dict()
    for line in open(args.schema, 'r'):
        table_to_col = line.strip().split(":")
        print(f"Table: {table_to_col[0]} -- Cols: {table_to_col[1]}")
        if table_to_col[0] not in schema:
            schema[table_to_col[0]] = []
            for match in re.finditer(r'([a-zA-Z]+_[a-zA-Z]+)\(([a-z]+)\)', table_to_col[1]):
                schema[table_to_col[0]].append((match.group(1),match.group(2)))

    Path(args.output).mkdir(parents=True, exist_ok=True)
    for table in progressbar(schema):
        df = pd.read_csv(f"{args.input}/{table}.tbl", delimiter='|', header=None)
        parallel_serialize(table, df, args.output, schema)
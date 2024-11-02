
# parquet-tools

Utility to pretty inspect Parquet files.

Supported reading from: http/https URL, local file, s3/s3a URL

## Features

- cat: Print the first N records from a file
- footer: Print the Parquet file footer in json format
- meta: Pretty Print a Parquet file's metadata
- schema: Pretty print the Avro schema for a file
- struct: Print the Go struct for a file
- diff: Diff two Parquet files schema

## Install

``` bash
go install github.com/jimyag/parquet-tools@latest
```

## Usage

``` bash
parquet-tools -h
```

pretty print the Avro schema for a file

``` bash
parquet-tools meta https://github.com/jimyag/parquet-tools/raw/main/testdata/v0.7.1.parquet
+-------------------------+------------------------------------------+
| KEY                     | VALUE                                    |
+-------------------------+------------------------------------------+
|         filename        | https://github.com/jimyag/parquet-tools/ |
|                         |     raw/main/testdata/v0.7.1.parquet     |
+-------------------------+------------------------------------------+
|         version         |                   v1.0                   |
+-------------------------+------------------------------------------+
|        created by       |    parquet-cpp version 1.3.2-SNAPSHOT    |
+-------------------------+------------------------------------------+
|         num rows        |                    10                    |
+-------------------------+------------------------------------------+
| key value file metadata |                     1                    |
+-------------------------+------------------------------------------+
|          pandas         | {"index_columns": ["__index_level_0__"], |
|                         |  "column_indexes": [{"name": null, "pand |
|                         | as_type": "string", "numpy_type": "objec |
|                         | t", "metadata": null}], "columns": [{"na |
|                         |  me": "carat", "pandas_type": "float64", |
|                         | "numpy_type": "float64", "metadata": nul |
|                         | l}, {"name": "cut", "pandas_type": "unic |
|                         | ode", "numpy_type": "object", "metadata" |
|                         | : null}, {"name": "color", "pandas_type" |
|                         | : "unicode", "numpy_type": "object", "me |
|                         | tadata": null}, {"name": "clarity", "pan |
|                         | das_type": "unicode", "numpy_type": "obj |
|                         | ect", "metadata": null}, {"name": "depth |
|                         | ", "pandas_type": "float64", "numpy_type |
|                         | ": "float64", "metadata": null}, {"name" |
|                         | : "table", "pandas_type": "float64", "nu |
|                         | mpy_type": "float64", "metadata": null}, |
|                         |  {"name": "price", "pandas_type": "int64 |
|                         | ", "numpy_type": "int64", "metadata": nu |
|                         | ll}, {"name": "x", "pandas_type": "float |
|                         | 64", "numpy_type": "float64", "metadata" |
|                         | : null}, {"name": "y", "pandas_type": "f |
|                         | loat64", "numpy_type": "float64", "metad |
|                         | ata": null}, {"name": "z", "pandas_type" |
|                         | : "float64", "numpy_type": "float64", "m |
|                         | etadata": null}, {"name": "__index_level |
|                         | _0__", "pandas_type": "int64", "numpy_ty |
|                         | pe": "int64", "metadata": null}], "panda |
|                         |           s_version": "0.20.1"}          |
+-------------------------+------------------------------------------+
|   number of row groups  |                     1                    |
+-------------------------+------------------------------------------+
|  number of real columns |                    11                    |
+-------------------------+------------------------------------------+
|    number of columns    |                    11                    |
+-------------------------+------------------------------------------+

--- row group:  0  begin ---
+----------------+------+
| total bytes    | 1327 |
+----------------+------+
| number of rows |   10 |
+----------------+------+
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
| COLUMN     | COUNT | MIN  | MAX  | NULLS | DISTI | COMPR | ENCOD | UNCOMPRESSED | COMPRESSED |
|            | S     |      |      |       | NCT   | ESSIO | INGS  |              |            |
|            |       |      |      |       |       | N     |       |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|    carat   |   10  | 0.21 | 0.31 |   0   |   0   | SNAPP | PLAIN | 125          | 129        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|     cut    |   10  |   -  |   -  |   0   |   0   | SNAPP | PLAIN | 115          | 119        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|    color   |   10  |   -  |   -  |   0   |   0   | SNAPP | PLAIN | 73           | 77         |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|   clarity  |   10  |   -  |   -  |   0   |   0   | SNAPP | PLAIN | 104          | 100        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|    depth   |   10  | 56.9 | 65.1 |   0   |   0   | SNAPP | PLAIN | 152          | 132        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|    table   |   10  |  55  |  65  |   0   |   0   | SNAPP | PLAIN | 109          | 105        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|    price   |   10  |  326 |  338 |   0   |   0   | SNAPP | PLAIN | 125          | 111        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|      x     |   10  | 3.87 | 4.34 |   0   |   0   | SNAPP | PLAIN | 145          | 143        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|      y     |   10  | 3.78 | 4.35 |   0   |   0   | SNAPP | PLAIN | 145          | 143        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
|      z     |   10  | 2.31 | 2.75 |   0   |   0   | SNAPP | PLAIN | 145          | 144        |
|            |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
| __index_le |   10  |   0  |   9  |   0   |   0   | SNAPP | PLAIN | 152          | 124        |
|   vel_0__  |       |      |      |       |       |   Y   | _DICT |              |            |
|            |       |      |      |       |       |       | IONAR |              |            |
|            |       |      |      |       |       |       | Y PLA |              |            |
|            |       |      |      |       |       |       | IN RL |              |            |
|            |       |      |      |       |       |       |   E   |              |            |
+------------+-------+------+------+-------+-------+-------+-------+--------------+------------+
--- row group:  0  end ---

required group field_id=-1 schema {
  optional double field_id=-1 carat;
  optional byte_array field_id=-1 cut (String);
  optional byte_array field_id=-1 color (String);
  optional byte_array field_id=-1 clarity (String);
  optional double field_id=-1 depth;
  optional double field_id=-1 table;
  optional int64 field_id=-1 price;
  optional double field_id=-1 x;
  optional double field_id=-1 y;
  optional double field_id=-1 z;
  optional int64 field_id=-1 __index_level_0__;
}
```

read from http or https

``` bash
parquet-tools schema https://github.com/jimyag/parquet-tools/raw/main/testdata/v0.7.1.parquet
required group field_id=-1 schema {
  optional double field_id=-1 carat;
  optional byte_array field_id=-1 cut (String);
  optional byte_array field_id=-1 color (String);
  optional byte_array field_id=-1 clarity (String);
  optional double field_id=-1 depth;
  optional double field_id=-1 table;
  optional int64 field_id=-1 price;
  optional double field_id=-1 x;
  optional double field_id=-1 y;
  optional double field_id=-1 z;
  optional int64 field_id=-1 __index_level_0__;
}
```

print the Go struct for a file

```bash
parquet-tools struct https://github.com/jimyag/parquet-tools/raw/main/testdata/v0.7.1.parquet
type schema struct {
  Carat any `parquet:"carat"`
  Cut string `parquet:"cut"`
  Color string `parquet:"color"`
  Clarity string `parquet:"clarity"`
  Depth any `parquet:"depth"`
  Table any `parquet:"table"`
  Price any `parquet:"price"`
  X any `parquet:"x"`
  Y any `parquet:"y"`
  Z any `parquet:"z"`
  IndexLevel0 any `parquet:"__index_level_0__"`
}
```

diff two Parquet files schema

```bash
parquet-tools diff v0.7.1.parquet v0.7.2.parquet
```

read from local file

``` bash
parquet-tools schema v0.7.1.parquet
required group field_id=-1 schema {
  optional double field_id=-1 carat;
  optional byte_array field_id=-1 cut (String);
  optional byte_array field_id=-1 color (String);
  optional byte_array field_id=-1 clarity (String);
  optional double field_id=-1 depth;
  optional double field_id=-1 table;
  optional int64 field_id=-1 price;
  optional double field_id=-1 x;
  optional double field_id=-1 y;
  optional double field_id=-1 z;
  optional int64 field_id=-1 __index_level_0__;
}
```

read from s3 bucket

``` bash
cat s3.toml
region = "xxxx"
access_key = "ak"
secret_key = "sk"
endpoint = "endpoint"
disable_ssl = false
force_path_style = true
```

``` bash
parquet-tools schema s3://jimyag/parquet-tools/testdata/v0.7.1.parquet --s3-config s3.toml
required group field_id=-1 schema {
  optional double field_id=-1 carat;
  optional byte_array field_id=-1 cut (String);
  optional byte_array field_id=-1 color (String);
  optional byte_array field_id=-1 clarity (String);
  optional double field_id=-1 depth;
  optional double field_id=-1 table;
  optional int64 field_id=-1 price;
  optional double field_id=-1 x;
  optional double field_id=-1 y;
  optional double field_id=-1 z;
  optional int64 field_id=-1 __index_level_0__;
}
```

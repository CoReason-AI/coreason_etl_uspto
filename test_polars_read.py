import sqlite3

import polars as pl

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE test (id int, name text)")
conn.execute("INSERT INTO test VALUES (1, 'Alice')")
conn.execute("INSERT INTO test VALUES (2, 'Bob')")
conn.commit()

query = "SELECT * FROM test"
df = pl.read_database(query, conn)
print(df)

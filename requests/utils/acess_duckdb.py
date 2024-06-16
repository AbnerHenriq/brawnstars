import duckdb

con = duckdb.connect('database/brawnstars.duckdb')
con.execute("DROP TABLE players")
print("Sucesso")
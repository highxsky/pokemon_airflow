import sqlite3
from pathlib import Path 

poke_dir = Path(__file__).parent.parent
db_path = poke_dir / 'db' / 'poke_db.db'
queries_dir = poke_dir / 'sql'
select_pokemon_details = queries_dir / 'select_pokemons.sql'

with sqlite3.connect(db_path) as conn:
    sql_query = select_pokemon_details.read_text()
    rows = conn.execute(sql_query)

for row in rows:
    print(row)
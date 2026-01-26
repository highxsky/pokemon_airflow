-- Create table listing pokemons
CREATE TABLE IF NOT EXISTS pokemons (
	id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
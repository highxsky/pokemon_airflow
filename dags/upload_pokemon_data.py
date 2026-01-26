#%%

# ==================================================
# About
# ==================================================

# DAG to fetch pokemons from the POKE API and upload it to a local SQLite db.
# Only 3 pokemons are ingested at a time.

# ==================================================
# TO DO
# ==================================================

# Clarify ingestion logic
    # One shot script to ingest all pokemons that exist for Gen 1 in pokemons table
    # 
    # Randomly select 3 pokemons to ingest at a time
        # Update table pokemon_details with data
    # Have an automated stored procedure
    # 
    # Ingest 3 pokemons at a time
    # Update list 

# ==================================================
# Approach
# ==================================================



# To store an actual img in a local DB: need a NOSQL database (e.g. Cassandra)
# KW = keyword
    # Kwargs = keyword 
# for variables in script, put inside function called in a dag task
    # airflow might send multiple calls otherwise, not optimal
# build a snowflake operator
    # hook and custom operator
# generate dynamic dags
# 

# Check airflow tests
    # https://www.astronomer.io/docs/learn/testing-airflow

# ==================================================
# Imports
# ==================================================

# packages
from airflow.sdk import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import date, datetime, timedelta
import logging
import requests
import json
import urllib.request
from pathlib import Path
import os

# ==================================================
# Setup
# ==================================================


# Constants
# ROOT_DIR = Path("opt/airflow/data/pokemon_dict")

# Logging setup

log = logging.getLogger(__name__)

# ==================================================
# Functions
# ==================================================

def get_pokemon_details(target_path_str: str) -> None:
    """Fetch details of a given Pokémon from the PokeAPI."""

    target_path = Path(target_path_str)

    pokemons = Variable.get(
        "pokemons",
        default=["pikachu", "raichu"],
        deserialize_json=True
    )

    pokemon_dict = {}

    for pokemon in pokemons:
        endpoint = f'https://pokeapi.co/api/v2/pokemon/{pokemon}/'
        try:
            resp = requests.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
            pokemon_dict[pokemon] = {
                "weight": data.get("weight"),
                "height": data.get("height"),
                "front_sprite_url": data.get("sprites").get("versions").get("generation-i").get("red-blue").get("front_default"),
                "back_sprite_url": data.get("sprites").get("versions").get("generation-i").get("red-blue").get("back_default")
            }
            logging.info(f"Fetched key data points for {pokemon}!")
        except requests.RequestException as e:
            logging.warning(f"Pokemon {pokemon}: does not exist!")

    # Ensure the directory exists
    target_path.mkdir(parents=True, exist_ok=True)

    # # Ensure the directory exists
    # if target_path.exists():
    #     None
    # else: 
    #     target_path.mkdir()
    
    # Creates a target path
    current_date = date.today().strftime("%Y-%m-%d")
    target_file_name = f"{current_date}_pokemon_dict.json"
    target_file_path = target_path / target_file_name

    with open(target_file_path, 'w') as json_file:
        try:
            json.dump(pokemon_dict, json_file, indent=4)
            logging.info(f"Successfully wrote pokemon dict as json file!")
        except:
            logging.error(f"Could not write pokemon dict as json file!")
            
    return None

def download_sprites(input_file: str, sprite_folder: str) -> None:

    with open(input_file) as file:
        # try:
        pokemon_dict = json.load(file)
        logging.info(f"Successfully read pokemon dict json file!")
        # except:
        #     logging.error(f"Could not read pokemon dict json file.")
        #     raise

    for pokemon, data in pokemon_dict.items():

        # create a sprite dict
        sprites = {
            "front": data.get("front_sprite_url"), 
            "back": data.get("back_sprite_url")
        }

        # create target folders
        sprite_target_folder = os.path.join(sprite_folder, pokemon)
        os.makedirs(sprite_target_folder, exist_ok=True)
        logging.info(f"Successfully created {pokemon} sub-folder!")
        # if not os.path.exists(sprite_target_folder):
        #     os.makedirs(sprite_target_folder)
        #     logging.info(f"Successfully created {pokemon} sub-folder!")

        # download sprites and write to target file
        for key, url in sprites.items():
            file_name = f"{pokemon}_{key}.png"
            sprite_target_file = os.path.join(sprite_target_folder, file_name)
            urllib.request.urlretrieve(url, sprite_target_file)
        logging.info(f"Successfully downloaded and stored {pokemon} sprites!")

    return None

# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="get_pokemon_data",
    start_date=datetime(2025, 11, 14),
    schedule='@daily',
    catchup=False,
    tags=["pokemon", "poke_api", "poke_db"]
) as dag:
    # 1st task - Extract
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=get_pokemon_details,
        op_kwargs={
            "target_path_str": "/opt/airflow/data/pokemon_dict"
        },
        retries=0,
        retry_delay=timedelta(minutes=1)
    )
    # 2nd task - Transform
    dl_sprites = PythonOperator(
        task_id="dl_sprites",
        python_callable=download_sprites,
        op_kwargs={ 
            "input_file": "/opt/airflow/data/pokemon_dict/{{ds}}_pokemon_dict.json",
            "sprite_folder": "/opt/airflow/data/sprites"
        },
        retries=0,
        retry_delay=timedelta(minutes=1)
    )

# ==================================================
# Workflow
# ==================================================

    extract_task >> dl_sprites

dag
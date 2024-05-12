import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import ast
import schedule
import time
import sys
from etl_titles import etl_process_titles
from etl_directors import etl_process_directors
from etl_actors import etl_process_actors
sys.path.append('../BI_HOMEWORK')

def run_etl_jobs():
    results = [
        etl_process_titles(),
        etl_process_actors(),
        etl_process_directors()
    ]
    
    return any(results)
 
def main():   
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="", port=5432)

    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS titles (
        id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        type VARCHAR(50),
        release_decade VARCHAR(20),
        age_certification VARCHAR(10) DEFAULT 'PG-0',
        runtime INT,
        genres VARCHAR[],
        production_countries VARCHAR[],
        seasons INT,
        imdb_id VARCHAR(20),
        imdb_score FLOAT,
        imdb_votes FLOAT,
        tmdb_popularity FLOAT,
        tmdb_score FLOAT,
        total_cast INT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS actors (
        person_id VARCHAR(255),
        id VARCHAR(255),
        name VARCHAR(500),
        character_name VARCHAR(500),
        PRIMARY KEY (person_id, id, character_name)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS directors (
        person_id VARCHAR(255),
        id VARCHAR(255),
        name VARCHAR(500),
        PRIMARY KEY (person_id, id)
    );
    """)

    conn.commit()

    cur.close()
    conn.close()

    schedule.every(3).seconds.do(run_etl_jobs)

    while True:
        schedule.run_pending()
        if not run_etl_jobs():
            print("ETL folyamatok befejeződtek, nincsen több adat.")
            break  # Kilép a ciklusból, ha nincs több adat
        time.sleep(1)

if __name__ == "__main__":
    main()

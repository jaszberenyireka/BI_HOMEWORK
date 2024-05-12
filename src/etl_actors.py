import pandas as pd
from sqlalchemy import create_engine
import os
import sys
sys.path.append('../BI_HOMEWORK')
   
def etl_process_actors():
    progress_file = 'progress_actors.txt'
    batch_size = 100
    
    # Ellenőrizzük, hogy van-e progress fájl, és olvassuk ki az utolsó feldolgozott sort
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            last_processed = int(file.read().strip())
    else:
        last_processed = 0
        
    # Adatok kinyerése
    credits_df = pd.read_csv('credits.csv', skiprows=range(1, last_processed+1), nrows=batch_size)
    
    # Ha nincs több sor a feldolgozáshoz
    if credits_df.empty:
        print("Nincsen több feldolgozandó adat.")
        return False
    
    # Adatok átalakítása
    # Színészek elkülönítése
    actors_df = credits_df[credits_df['role'] == 'ACTOR'].copy()

    actors_df.rename(columns={'character': 'character_name'}, inplace=True)
    actors_df['character_name'].fillna('Unknown', inplace=True)

    # A role oszlop eltávolítása a DataFrame-ből
    actors_df.drop(columns=['role'], inplace=True)

    # Adatok betöltése az 'actors' táblába
    engine = create_engine('postgresql+psycopg2://postgres:@localhost:5432/postgres')
    
    actors_df.to_sql('actors', con=engine, if_exists='append', index=False)
    
    # Frissítjük a progress fájlt az utolsó feldolgozott sorral
    last_processed += len(credits_df)
    with open(progress_file, 'w') as file:
        file.write(str(last_processed))

    print(f"Adatok sikeresen betöltve. Utolsó feldolgozott sor: {last_processed}")
    return True

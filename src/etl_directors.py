import pandas as pd
from sqlalchemy import create_engine
import os
import sys
sys.path.append('../BI_HOMEWORK')
   
def etl_process_directors():
    progress_file = 'progress_directors.txt'
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
    # Rendezők elkülönítése
    directors_df = credits_df[credits_df['role'] == 'DIRECTOR'].copy()

    # A character oszlop eltávolítása a DataFrame-ből
    directors_df.drop(columns=['character'], inplace=True)

    # A role oszlop eltávolítása a DataFrame-ből
    directors_df.drop(columns=['role'], inplace=True)

    # Adatok betöltése a 'directors' táblába
    engine = create_engine('postgresql+psycopg2://postgres:Bb20230906r@localhost:5432/postgres')
    
    directors_df.to_sql('directors', con=engine, if_exists='append', index=False)
    
    # Frissítjük a progress fájlt az utolsó feldolgozott sorral
    last_processed += len(credits_df)
    with open(progress_file, 'w') as file:
        file.write(str(last_processed))

    print(f"Adatok sikeresen betöltve. Utolsó feldolgozott sor: {last_processed}")
    return True

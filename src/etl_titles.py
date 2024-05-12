import pandas as pd
from sqlalchemy import create_engine
import os
import ast
import sys
sys.path.append('../BI_HOMEWORK')

def convert_array(data):
    try:
        data_list = ast.literal_eval(data)
        return "{" + ",".join(data_list) + "}"
    except:
        return "{}"
    
def etl_process_titles():
    progress_file = 'progress_titles.txt'
    batch_size = 100
    
    # Ellenőrizzük, hogy van-e progress fájl, és olvassuk ki az utolsó feldolgozott sort
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            last_processed = int(file.read().strip())
    else:
        last_processed = 0
        
    # Adatok kinyerése
    titles_df = pd.read_csv('titles.csv', skiprows=range(1, last_processed+1), nrows=batch_size)
    credits_df = pd.read_csv('credits.csv')
    
    # Ha nincs több sor a feldolgozáshoz
    if titles_df.empty:
        print("Nincsen több feldolgozandó adat.")
        return False
    
    # Adatok átalakítása
    titles_df['genres'] = titles_df['genres'].apply(convert_array)
    titles_df['production_countries'] = titles_df['production_countries'].apply(convert_array)

    # Ha az age_certification érték NULL, lecserélem 'PG-0'-ra
    titles_df['age_certification'].fillna('PG-0', inplace=True)

    # A description oszlop eltávolítása a DataFrame-ből
    titles_df.drop(columns=['description'], inplace=True)

    # Évszámok évtizedekbe sorolása
    titles_df['release_decade'] = (titles_df['release_year'] // 10 * 10).astype(str)
    titles_df.drop(columns=['release_year'], inplace=True)

    # Összegyűjti, hány személy dolgozott az egyes filmeken
    cast_counts = credits_df.groupby('id').size()
    titles_df = titles_df.merge(cast_counts.rename('total_cast'), on='id', how='left')
    titles_df['total_cast'].fillna(0, inplace=True)
    titles_df['total_cast'] = titles_df['total_cast'].astype(int)

    # Adatok betöltése a 'titles' táblába
    engine = create_engine('postgresql+psycopg2://postgres:Bb20230906r@localhost:5432/postgres')

    titles_df.to_sql('titles', con=engine, if_exists='append', index=False)
    
    # Frissítjük a progress fájlt az utolsó feldolgozott sorral
    last_processed += len(titles_df)
    with open(progress_file, 'w') as file:
        file.write(str(last_processed))

    print(f"Adatok sikeresen betöltve. Utolsó feldolgozott sor: {last_processed}")
    return True

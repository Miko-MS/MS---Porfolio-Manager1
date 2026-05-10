# MS---Porfolio-Manager1
Manager for Multiple portfolios
import yfinance as yf
import numpy as np
import pandas as pd
import sqlite3 as sql
# 10 spolek NASDAQ 100 o najwiekszej kapitalizacji (stan ~2025)
nasdaq_top10 = {
    '^VIX':  'CBOE Volatility Index',
    'AAPL':  'Apple',
    'MSFT':  'Microsoft',
    'NVDA':  'NVIDIA',
    'AMZN':  'Amazon',
    'META':  'Meta Platforms',
    'GOOGL': 'Alphabet',
    'AVGO':  'Broadcom',
    'TSLA':  'Tesla',
    'COST':  'Costco',
    'NFLX':  'Netflix'
}

tickers = list(nasdaq_top10.keys())
print('Tickery do pobrania:', tickers)

# Pobieramy dzienne ceny zamkniecia za okres 2020-2025
data_od = '2004-01-01'
data_do = '2025-12-31'

df_all = yf.download(
    tickers=tickers,
    start=data_od,
    end=data_do,
    group_by='ticker',  # dane pogrupowane po tickerze
    auto_adjust=True     # ceny skorygowane o splity / dywidendy
)

print('Ksztalt danych:', df_all.shape)
df_all.head()


frames = []
for ticker in tickers:
    tmp = df_all[ticker].copy()
    tmp['Ticker'] = ticker
    tmp['Company'] = nasdaq_top10[ticker]
    tmp.index.name = 'Date'
    tmp = tmp.reset_index()
    frames.append(tmp)

df = pd.concat(frames, ignore_index=True)

# Usuwamy wiersze bez ceny zamkniecia
df = df.dropna(subset=['Close'])

print(f'Laczna liczba wierszy: {len(df)}')
print(f'Unikalne tickery:      {df["Ticker"].nunique()}')
df.head()

# Tworzymy (lub otwieramy) plik bazy danych
conn = sql.connect('nasdaq_top10.db')

# Zapisujemy DataFrame do tabeli SQL
df.to_sql('prices', conn, if_exists='replace', index=False)

print('Tabela "prices" zapisana w pliku nasdaq_top10.db')

# Wczytanie calej tabeli
df_sql = pd.read_sql('SELECT * FROM prices', conn)
print(f'Wierszy: {len(df_sql)}, Kolumn: {len(df_sql.columns)}')
df_sql.head()

# Filtrowanie SQL – tylko Apple, rok 2024
query = """
SELECT Date, Close
FROM prices
WHERE Ticker = '^VIX'
  AND Date >= '2024-01-01'
  AND Date <  '2025-01-01'
ORDER BY Date
"""

df_vix_2024 = pd.read_sql(query, conn)
print(f'Wierszy ^VIX 2024: {len(df_vix_2024)}')
df_vix_2024.head()


# Definicja portfela (poprawiony ticker AAPL)
portfolio = {
    'AAPL':  'Apple',
    'MSFT':  'Microsoft',
    'NVDA':  'NVIDIA',
    'AMZN':  'Amazon',
    'META':  'Meta Platforms',
    'GOOGL': 'Alphabet',
    'AVGO':  'Broadcom',
    'TSLA':  'Tesla',
    'COST':  'Costco',
    'NFLX':  'Netflix'
}

# 1. Parametry początkowe
kapital_startowy = 10000  # USD
liczba_spolek = len(portfolio)

# 2. Obliczenia alokacji (dynamicznie, na wypadek gdybyś kiedyś usunął/dodał spółkę)
waga_pojedyncza = 1.0 / liczba_spolek          # Wynik: 0.10 (czyli 10%)
kapital_na_spolke = kapital_startowy * waga_pojedyncza # Wynik: 1000 USD

# 3. Zbudowanie tabeli (DataFrame) podsumowującej start
# Używamy orient='index', aby klucze słownika (tickery) stały się indeksami (nazwami wierszy)
df_start = pd.DataFrame.from_dict(portfolio, orient='index', columns=['Nazwa_Spolki'])

# 4. Dodanie kolumn z obliczeniami
df_start['Waga_Procentowa'] = waga_pojedyncza * 100
df_start['Kapital_Startowy_USD'] = kapital_na_spolke

# Wyświetlenie podsumowania
print(f"--- PARAMETRY PORTFELA ---")
print(f"Kapitał całkowity: {kapital_startowy:,.2f} USD")
print(f"Liczba walorów:    {liczba_spolek}")
print(f"Alokacja na walor: {kapital_na_spolke:,.2f} USD ({waga_pojedyncza*100}%)\n")

print("--- STAN PORTFELA (DZIEŃ 0) ---")
print(df_start)

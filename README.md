# MS---Porfolio-Manager1
import yfinance as yf
import pandas as pd
import sqlite3

# 10 spolek NASDAQ 100 + VIX
nasdaq_top10V2 = {
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

data_od = '2012-05-18'
data_do = '2025-12-31'

frames = []

print("Rozpoczynam pobieranie danych...")

# 1. POBIERANIE W PĘTLI Z ZABEZPIECZENIAMI
for ticker, company_name in nasdaq_top10V2.items():
    print(f"Pobieranie: {ticker} ({company_name})...", end=" ")
    
    try:
        # Pobieramy dane tylko dla jednego tickera naraz (progress=False ukrywa pasek ładowania yf)
        tmp = yf.download(ticker, start=data_od, end=data_do, auto_adjust=True, progress=False)
        
        # 2. WERYFIKACJA CZY DANE ISTNIEJĄ
        if tmp.empty:
            print("❌ BŁĄD: Brak danych dla tego tickera w wybranym okresie.")
            continue # Przejdź do następnej spółki
            
        # W nowszych wersjach yfinance, nawet pojedynczy ticker może zwrócić MultiIndex.
        # Ta linijka upewnia się, że kolumny są płaskie (Open, High, Low, Close, Volume)
        if isinstance(tmp.columns, pd.MultiIndex):
            tmp.columns = tmp.columns.droplevel(1)
            
        # Dodajemy kolumny identyfikacyjne
        tmp['Ticker'] = ticker
        tmp['Company'] = company_name
        tmp.index.name = 'Date'
        tmp = tmp.reset_index()
        
        # 3. CZYSZCZENIE DANYCH
        # Usuwamy wiersze bez ceny zamknięcia (np. błędy API lub dni wolne)
        tmp = tmp.dropna(subset=['Close'])
        
        frames.append(tmp)
        print(f"✅ Sukces (Wierszy: {len(tmp)})")
        
    except Exception as e:
        # Jeśli API rzuci błędem (np. brak internetu, odrzucenie zapytania), skrypt nie umrze!
        print(f"❌ BŁĄD KRYTYCZNY: {e}")

# 4. ŁĄCZENIE I ZAPIS DO BAZY DANYCH
if not frames:
    print("\nSKRYPT ZATRZYMANY: Nie udało się pobrać danych dla żadnej spółki.")
else:
    # Łączymy wszystkie udane pobrania w jedną tabelę
    df = pd.concat(frames, ignore_index=True)
    
    print('\n--- PODSUMOWANIE ---')
    print(f'Laczna liczba wierszy: {len(df)}')
    print(f'Unikalne tickery:      {df["Ticker"].nunique()}')
    
    # Używamy bloku "with", który sam bezpiecznie zamyka połączenie z bazą
    with sqlite3.connect('nasdaq_top10.db') as conn:
        df.to_sql('prices', conn, if_exists='replace', index=False)
        print('Tabela "prices" bezpiecznie zapisana w pliku nasdaq_top10.db\n')
        
        # TEST: Wczytanie całej tabeli
        df_sql = pd.read_sql('SELECT * FROM prices', conn)
        print(f'Wczytano z bazy wierszy: {len(df_sql)}, Kolumn: {len(df_sql.columns)}')
        
# TEST: Filtrowanie SQL – tylko ^VIX, rok 2024
query = """
SELECT Date, Close
FROM prices
WHERE Ticker = '^VIX'
AND Date >= '2024-01-01'
AND Date <  '2025-01-01'
ORDER BY Date
"""
df_vix_2024 = pd.read_sql(query, conn)
print(f'Wczytano z bazy wierszy ^VIX 2024: {len(df_vix_2024)}')
print("--- Pierwsze 5 wierszy tabeli: ---")
print(df_vix_2024.head())


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

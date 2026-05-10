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


# 1. Definicja parametrów początkowych
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

kapital_startowy_calkowity = 10000.0  # USD
waga_poczatkowa_proc = 100.0 / len(portfolio)
kapital_na_spolke = kapital_startowy_calkowity / len(portfolio)

# 2. Pobranie danych z bazy SQL i przekształcenie w Pivot Table
with sqlite3.connect('nasdaq_top10.db') as conn:
    df_baza = pd.read_sql('SELECT Date, Ticker, Close FROM prices', conn)

df_baza['Date'] = pd.to_datetime(df_baza['Date'])
df_pivot = df_baza.pivot(index='Date', columns='Ticker', values='Close')

# 3. ZNAJDOWANIE CEN I DAT (Odporność na brakujące dane!)
# .iloc[0] wyciąga pierwszą dostępną cenę
ceny_poczatkowe = df_pivot.apply(lambda col: col.dropna().iloc[0])
ceny_koncowe = df_pivot.apply(lambda col: col.dropna().iloc[-1])

# .index[0] wyciąga datę przypisaną do tej pierwszej dostępnej ceny
daty_poczatkowe = df_pivot.apply(lambda col: col.dropna().index[0])

# 4. Obliczenia dla poszczególnych spółek
wyniki = []

for ticker, nazwa in portfolio.items():
    cena_pocz = ceny_poczatkowe[ticker]
    cena_konc = ceny_koncowe[ticker]
    data_dodania = daty_poczatkowe[ticker]  # <-- Wyciągnięcie daty
    
    stopa_zwrotu = ((cena_konc / cena_pocz) - 1) * 100
    kapital_koncowy = kapital_na_spolke * (1 + (stopa_zwrotu / 100))
    
    wyniki.append({
        'Ticker': ticker,
        'Nazwa_Akcji': nazwa,
        'Data_Dodania': data_dodania,       # <-- Dodanie do tabeli
        'Waga_Pocz_%': waga_poczatkowa_proc,
        'Kapital_Startowy_USD': kapital_na_spolke,
        'Cena_Pocz_USD': cena_pocz,
        'Cena_Konc_USD': cena_konc,
        'Stopa_Zwrotu_%': stopa_zwrotu,
        'Kapital_Koncowy_USD': kapital_koncowy
    })

df_wyniki = pd.DataFrame(wyniki)

# 5. Podsumowanie całego portfela
portfel_start = df_wyniki['Kapital_Startowy_USD'].sum()
portfel_koniec = df_wyniki['Kapital_Koncowy_USD'].sum()
portfel_zwrot = ((portfel_koniec / portfel_start) - 1) * 100

# ==========================================
# 6. ESTETYKA WYSWIETLANIA (Formatowanie)
# ==========================================
df_wyswietlanie = df_wyniki.copy()

# Dodajemy formatowanie daty na standardowy i czytelny rrrr-mm-dd
format_daty = lambda x: x.strftime('%Y-%m-%d')
format_waluta = lambda x: f"${x:,.2f}"
format_procent = lambda x: f"{x:,.2f}%"

df_wyswietlanie['Data_Dodania'] = df_wyswietlanie['Data_Dodania'].apply(format_daty)
df_wyswietlanie['Waga_Pocz_%'] = df_wyswietlanie['Waga_Pocz_%'].apply(format_procent)
df_wyswietlanie['Kapital_Startowy_USD'] = df_wyswietlanie['Kapital_Startowy_USD'].apply(format_waluta)
df_wyswietlanie['Cena_Pocz_USD'] = df_wyswietlanie['Cena_Pocz_USD'].apply(format_waluta)
df_wyswietlanie['Cena_Konc_USD'] = df_wyswietlanie['Cena_Konc_USD'].apply(format_waluta)
df_wyswietlanie['Stopa_Zwrotu_%'] = df_wyswietlanie['Stopa_Zwrotu_%'].apply(format_procent)
df_wyswietlanie['Kapital_Koncowy_USD'] = df_wyswietlanie['Kapital_Koncowy_USD'].apply(format_waluta)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

print("--- WYNIKI INDYWIDUALNE SPÓŁEK ---")
print(df_wyswietlanie.to_string(index=False))

print("\n--- PODSUMOWANIE PORTFELA ---")
print(f"Kapitał Początkowy:     ${portfel_start:,.2f}")
print(f"Kapitał Końcowy:        ${portfel_koniec:,.2f}")
print(f"Całkowita Stopa Zwrotu: {portfel_zwrot:,.2f}%\n")

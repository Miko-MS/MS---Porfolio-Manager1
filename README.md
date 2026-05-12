import yfinance as yf
import numpy as np
import pandas as pd
import sqlite3 
# ============================================================
#  KONFIGURACJA  (jedno miejsce – łatwa edycja)
# ============================================================
 
# Portfel inwestycyjny – 10 spółek NASDAQ
PORTFOLIO = {
    'AAPL':  'Apple',
    'MSFT':  'Microsoft',
    'NVDA':  'NVIDIA',
    'AMZN':  'Amazon',
    'META':  'Meta Platforms',
    'GOOGL': 'Alphabet',
    'AVGO':  'Broadcom',
    'TSLA':  'Tesla',
    'COST':  'Costco',
    'NFLX':  'Netflix',
}
 
# Indeksy pomocnicze – nie wchodzą do portfela
INDEKSY = {
    '^VIX': 'CBOE Volatility Index',
}
 
DATA_OD          = '2012-05-18'
DATA_DO          = '2025-12-31'
KAPITAL_USD      = 10_000.0
DB_PATH          = 'nasdaq_top10.db'
DB_TABLE         = 'prices'
 
# Wszystkie tickery do pobrania (portfel + indeksy)
WSZYSTKIE_TICKERY = {**PORTFOLIO, **INDEKSY}
 
 
# ============================================================
#  MODUŁ I – Pobieranie danych i zapis do SQLite
# ============================================================
 
def pobierz_dane(tickery: dict, data_od: str, data_do: str) -> pd.DataFrame:
    """
    Pobiera dane historyczne dla podanego słownika tickerów.
    Zwraca połączony DataFrame ze wszystkimi spółkami.
    """
    frames = []
 
    print("=" * 50)
    print("MODUŁ I – Pobieranie danych")
    print("=" * 50)
 
    for ticker, nazwa in tickery.items():
        print(f"  Pobieranie: {ticker:6s} ({nazwa})...", end=" ")
 
        try:
            tmp = yf.download(
                ticker,
                start=data_od,
                end=data_do,
                auto_adjust=True,   # ceny uwzględniają dywidendy i splity
                progress=False,
            )
 
            if tmp.empty:
                print("❌  Brak danych – ticker pominięty.")
                continue
 
            # Nowsze wersje yfinance mogą zwrócić MultiIndex – spłaszczamy
            if isinstance(tmp.columns, pd.MultiIndex):
                tmp.columns = tmp.columns.droplevel(1)
 
            tmp['Ticker']  = ticker
            tmp['Company'] = nazwa
            tmp.index.name = 'Date'
            tmp = tmp.reset_index()
            tmp = tmp.dropna(subset=['Close'])   # usuwamy wiersze bez kursu
 
            frames.append(tmp)
            print(f"✅  {len(tmp)} wierszy")
 
        except Exception as e:
            print(f"❌  Błąd krytyczny: {e}")
 
    if not frames:
        raise RuntimeError("Nie pobrano danych dla żadnego tickera. Sprawdź połączenie.")
 
    return pd.concat(frames, ignore_index=True)
 
 
def zapisz_do_bazy(df: pd.DataFrame, db_path: str, table: str) -> None:
    """Zapisuje DataFrame do tabeli SQLite (nadpisuje istniejącą)."""
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists='replace', index=False)
    print(f"\n✅  Tabela '{table}' zapisana w pliku '{db_path}'")
    print(f"    Łącznie wierszy: {len(df)} | Tickerów: {df['Ticker'].nunique()}\n")
 
 
# ============================================================
#  MODUŁ II – Budowa portfela i obliczanie stóp zwrotu
# ============================================================
 
def wczytaj_pivot(db_path: str, table: str, tickery: list) -> pd.DataFrame:
    """
    Wczytuje kursy zamknięcia z bazy i zwraca pivot table.
    Kolumny = tickery, indeks = daty (datetime).
    """
    tickers_sql = ", ".join(f"'{t}'" for t in tickery)
    query = f"""
        SELECT Date, Ticker, Close
        FROM {table}
        WHERE Ticker IN ({tickers_sql})
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(query, conn, parse_dates=['Date'])
 
    return df.pivot(index='Date', columns='Ticker', values='Close')
 
 
def oblicz_portfel(pivot: pd.DataFrame, portfolio: dict, kapital: float) -> pd.DataFrame:
    """
    Oblicza wyniki portfela równoważonego (equal-weight).
 
    Stopa zwrotu dla każdej spółki:
        R = (cena_końcowa / cena_startowa - 1) × 100 [%]
 
    Przyjmuje WSPÓLNĄ datę startową = pierwszy dzień, gdy WSZYSTKIE
    spółki mają notowania (dropna na wierszach pivot). Dzięki temu
    porównanie między spółkami jest sprawiedliwe.
    """
    # Wspólna data startowa – pierwszy wiersz bez żadnych NaN
    pivot_kompletny = pivot[list(portfolio.keys())].dropna()
 
    if pivot_kompletny.empty:
        raise ValueError("Brak wspólnych dat notowań dla wszystkich spółek portfela.")
 
    data_start = pivot_kompletny.index[0]
    data_koniec = pivot_kompletny.index[-1]
 
    ceny_start = pivot_kompletny.iloc[0]
    ceny_koniec = pivot_kompletny.iloc[-1]
 
    waga         = 100.0 / len(portfolio)          # % na spółkę
    kapital_1    = kapital / len(portfolio)         # USD na spółkę
 
    wyniki = []
    for ticker, nazwa in portfolio.items():
        stopa   = (ceny_koniec[ticker] / ceny_start[ticker] - 1) * 100
        kap_end = kapital_1 * (1 + stopa / 100)
 
        wyniki.append({
            'Ticker':              ticker,
            'Spółka':              nazwa,
            'Data_Start':          data_start.date(),
            'Data_Koniec':         data_koniec.date(),
            'Waga_%':              waga,
            'Kapitał_Start_USD':   kapital_1,
            'Cena_Start_USD':      ceny_start[ticker],
            'Cena_Koniec_USD':     ceny_koniec[ticker],
            'Stopa_Zwrotu_%':      stopa,
            'Kapitał_Koniec_USD':  kap_end,
        })
 
    return pd.DataFrame(wyniki)
 
 
def drukuj_wyniki(df: pd.DataFrame) -> None:
    """Formatuje i drukuje tabelę wyników oraz podsumowanie portfela."""
 
    # Formatowanie kolumn liczbowych – tylko do wyświetlania
    fmt = {
        'Waga_%':             '{:,.2f}%'.format,
        'Kapitał_Start_USD':  '${:,.2f}'.format,
        'Cena_Start_USD':     '${:,.2f}'.format,
        'Cena_Koniec_USD':    '${:,.2f}'.format,
        'Stopa_Zwrotu_%':     '{:,.2f}%'.format,
        'Kapitał_Koniec_USD': '${:,.2f}'.format,
    }
 
    df_display = df.copy()
    for col, func in fmt.items():
        df_display[col] = df_display[col].apply(func)
 
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1200)
 
    print("=" * 50)
    print("MODUŁ II – Wyniki portfela")
    print("=" * 50)
    print(df_display.to_string(index=False))
 
    # Podsumowanie na liczbach oryginalnych (nie sformatowanych)
    kap_start  = df['Kapitał_Start_USD'].sum()
    kap_koniec = df['Kapitał_Koniec_USD'].sum()
    zwrot      = (kap_koniec / kap_start - 1) * 100
 
    print("\n--- PODSUMOWANIE PORTFELA ---")
    print(f"  Kapitał początkowy:      ${kap_start:>12,.2f}")
    print(f"  Kapitał końcowy:         ${kap_koniec:>12,.2f}")
    print(f"  Całkowita stopa zwrotu:  {zwrot:>12,.2f}%\n")
 
 
# ============================================================
#  PUNKT WEJŚCIA
# ============================================================
 
if __name__ == '__main__':
 
    # --- MODUŁ I ---
    df_raw = pobierz_dane(WSZYSTKIE_TICKERY, DATA_OD, DATA_DO)
    zapisz_do_bazy(df_raw, DB_PATH, DB_TABLE)
 
    # Podgląd VIX za rok 2024 (przykład zapytania SQL)
    with sqlite3.connect(DB_PATH) as conn:
        df_vix = pd.read_sql("""
            SELECT Date, Close
            FROM prices
            WHERE Ticker = '^VIX'
              AND Date >= '2024-01-01'
              AND Date <  '2025-01-01'
            ORDER BY Date
        """, conn)
    print(f"VIX 2024 – wierszy: {len(df_vix)}")
    print(df_vix.head().to_string(index=False), "\n")
 
    # --- MODUŁ II ---
    pivot = wczytaj_pivot(DB_PATH, DB_TABLE, list(PORTFOLIO.keys()))
    df_wyniki = oblicz_portfel(pivot, PORTFOLIO, KAPITAL_USD)
    drukuj_wyniki(df_wyniki)



# ============================================================
#  MODUŁ III – Roczne stopy zwrotu portfela + CAGR 
# ============================================================

# ── konfiguracja (zgodna z poprzednimi modułami) ────────────────────────────
 
PORTFOLIO = {
    'AAPL':  'Apple',
    'MSFT':  'Microsoft',
    'NVDA':  'NVIDIA',
    'AMZN':  'Amazon',
    'META':  'Meta Platforms',
    'GOOGL': 'Alphabet',
    'AVGO':  'Broadcom',
    'TSLA':  'Tesla',
    'COST':  'Costco',
    'NFLX':  'Netflix',
}
 
DB_PATH  = 'nasdaq_top10.db'
DB_TABLE = 'prices'
LATA     = list(range(2012, 2026))   # 2012–2025
 
 
# ── pomocnicze ──────────────────────────────────────────────────────────────
 
def ostatni_kurs_roku(pivot: pd.DataFrame, rok: int) -> pd.Series | None:
    """Zwraca kursy z ostatniego dostępnego dnia notowań w danym roku."""
    maska = pivot.index.year == rok
    if not maska.any():
        return None
    return pivot.loc[maska].ffill().iloc[-1]   # ostatni dzień z kursem
 
 
def cagr(cena_start: float, cena_koniec: float, n_lat: int) -> float:
    """CAGR = (cena_koniec / cena_start)^(1/n) - 1  [w %]"""
    if n_lat <= 0 or cena_start <= 0:
        return np.nan
    return ((cena_koniec / cena_start) ** (1 / n_lat) - 1) * 100
 
 
# ── główna funkcja ──────────────────────────────────────────────────────────
 
def buduj_tabele_roczna(db_path: str, table: str,
                        portfolio: dict, lata: list) -> pd.DataFrame:
    """
    Buduje tabelę rocznych stóp zwrotu.
 
    Stopa zwrotu w roku t:
        R_t = (P_koniec_t / P_koniec_(t-1) - 1) × 100
 
    Dla pierwszego roku (np. 2012) bazą jest kurs z dnia IPO / pierwszego
    dostępnego notowania (może być w środku roku), więc stopa zwrotu
    jest za niepełny rok – oznaczamy to w nagłówku gwiazdką.
 
    CAGR liczymy od PIERWSZEGO dostępnego kursu do OSTATNIEGO dostępnego kursu.
    """
 
    # 1. wczytaj dane z bazy
    tickers_sql = ", ".join(f"'{t}'" for t in portfolio)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(
            f"SELECT Date, Ticker, Close FROM {table} WHERE Ticker IN ({tickers_sql})",
            conn, parse_dates=['Date']
        )
 
    pivot = df.pivot(index='Date', columns='Ticker', values='Close').sort_index()
 
    # 2. kursy na koniec każdego roku (+ kurs bazowy dla roku t-1)
    kursy_rok = {}
    for rok in lata:
        kursy_rok[rok] = ostatni_kurs_roku(pivot, rok)
 
    # kurs bazowy dla pierwszego roku = pierwszy dostępny kurs każdej spółki
    kursy_rok[lata[0] - 1] = pivot.apply(lambda col: col.dropna().iloc[0])
 
    # 3. oblicz roczne stopy zwrotu
    wiersze = {}
 
    for ticker, nazwa in portfolio.items():
        row = {}
        for rok in lata:
            kurs_t   = kursy_rok.get(rok)
            kurs_t1  = kursy_rok.get(rok - 1)
 
            if kurs_t is None or kurs_t1 is None:
                row[rok] = np.nan
                continue
 
            p_end   = kurs_t.get(ticker,   np.nan)
            p_start = kurs_t1.get(ticker,  np.nan)
 
            if pd.isna(p_end) or pd.isna(p_start) or p_start == 0:
                row[rok] = np.nan
            else:
                row[rok] = (p_end / p_start - 1) * 100
 
        # CAGR – od pierwszego do ostatniego dostępnego kursu
        ceny_spolki  = pivot[ticker].dropna()
        n_lat_aktual = (ceny_spolki.index[-1] - ceny_spolki.index[0]).days / 365.25
        row['CAGR'] = cagr(ceny_spolki.iloc[0], ceny_spolki.iloc[-1], n_lat_aktual)
 
        wiersze[nazwa] = row
 
    # 4. wiersz portfela (equal-weight – średnia stóp zwrotu spółek)
    df_temp = pd.DataFrame(wiersze).T
    portfel_row = {}
    for rok in lata:
        portfel_row[rok] = df_temp[rok].mean()          # śr. arytm. stóp
    portfel_row['CAGR'] = df_temp['CAGR'].mean()        # śr. CAGR
 
    wiersze['📊 PORTFEL (equal-weight)'] = portfel_row
 
    # 5. składamy finalną tabelę
    df_wynik = pd.DataFrame(wiersze).T
    df_wynik.index.name = 'Spółka'
 
    return df_wynik
df_roczne = buduj_tabele_roczna(DB_PATH, DB_TABLE, PORTFOLIO, LATA)
print(df_roczne.to_string())


# ============================================================
#  Moduł IV – Wartość portfela i wagi akcji na koniec każdego roku
# ============================================================

# ── 1. wczytaj kursy i zbuduj pivot ─────────────────────────────────────────
 
tickers_sql = ", ".join(f"'{t}'" for t in PORTFOLIO)
with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql(
        f"SELECT Date, Ticker, Close FROM {DB_TABLE} WHERE Ticker IN ({tickers_sql})",
        conn, parse_dates=['Date']
    )
 
pivot = df.pivot(index='Date', columns='Ticker', values='Close').sort_index()
 
 
# ── 2. kurs na koniec każdego roku ──────────────────────────────────────────
 
def kurs_koniec_roku(pivot, rok):
    """Ostatni dostępny kurs w danym roku."""
    maska = pivot.index.year == rok
    if not maska.any():
        return None
    return pivot.loc[maska].ffill().iloc[-1]
 
 
kursy = {}
for rok in LATA:
    k = kurs_koniec_roku(pivot, rok)
    if k is not None:
        kursy[rok] = k
 
# kurs bazowy = pierwszy dostępny kurs każdej spółki (przed rokiem 2012)
kursy[LATA[0] - 1] = pivot.apply(lambda col: col.dropna().iloc[0])
 
 
# ── 3. wartości pozycji i wagi ───────────────────────────────────────────────
# Każda spółka startuje z kapitałem = KAPITAL_USD / liczba spółek
# Wartość pozycji na koniec roku t:
#     V_t = V_start * (P_t / P_start)
# gdzie P_start = pierwszy dostępny kurs spółki
 
kapital_na_spolke = KAPITAL_USD / len(PORTFOLIO)
 
wartosci = {}   # wartosci[rok][ticker] = wartość pozycji w USD
wagi     = {}   # wagi[rok][ticker]     = udział w portfelu w %
 
for rok in LATA:
    if rok not in kursy:
        continue
 
    row_wartosci = {}
    for ticker in PORTFOLIO:
        p_start = kursy[LATA[0] - 1][ticker]   # pierwszy kurs spółki
        p_koniec = kursy[rok][ticker]
        if pd.isna(p_start) or pd.isna(p_koniec) or p_start == 0:
            row_wartosci[ticker] = None
        else:
            row_wartosci[ticker] = kapital_na_spolke * (p_koniec / p_start)
 
    wartosci[rok] = row_wartosci
 
    # suma portfela i wagi
    suma = sum(v for v in row_wartosci.values() if v is not None)
    wagi[rok] = {
        ticker: (v / suma * 100 if v is not None else None)
        for ticker, v in row_wartosci.items()
    }
 
 
# ── 4. budowa tabel wynikowych ───────────────────────────────────────────────
 
df_wartosci = pd.DataFrame(wartosci).T          # wiersze=lata, kolumny=tickery
df_wartosci.index.name = 'Rok'
 
# dodaj wiersz sumy portfela
df_wartosci['PORTFEL_USD'] = df_wartosci.sum(axis=1)
 
df_wagi = pd.DataFrame(wagi).T
df_wagi.index.name = 'Rok'
 
# zmień nazwy kolumn na pełne nazwy spółek
rename = {t: n for t, n in PORTFOLIO.items()}
df_wartosci_display = df_wartosci.rename(columns=rename)
df_wagi_display     = df_wagi.rename(columns=rename)
 
 
# ── 5. wyświetlenie ──────────────────────────────────────────────────────────
 
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1600)
pd.set_option('display.float_format', '{:,.2f}'.format)
 
print("=" * 60)
print("TABELA 1 – Wartość pozycji na koniec roku [USD]")
print("=" * 60)
print(df_wartosci_display.to_string())
 
print("\n")
print("=" * 60)
print("TABELA 2 – Udział akcji w portfelu na koniec roku [%]")
print("=" * 60)
print(df_wagi_display.to_string())


# ============================================================
#  MODUŁ V – Wizualizacja - wykres wartości portfela w czasie 
# ============================================================


import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates

# ── 1. wczytaj kursy z bazy ──────────────────────────────────────────────────

def wczytaj_kursy(db_path: str, table: str, tickery: list) -> pd.DataFrame:
    tickers_sql = ", ".join(f"'{t}'" for t in tickery)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(
            f"SELECT Date, Ticker, Close FROM {table} WHERE Ticker IN ({tickers_sql})",
            conn, parse_dates=['Date']
        )
    pivot = df.pivot(index='Date', columns='Ticker', values='Close').sort_index()
    return pivot


# ── 2. oblicz wartość portfela w czasie ──────────────────────────────────────

def oblicz_wartosc_portfela(pivot: pd.DataFrame, portfolio: dict, kapital: float) -> pd.DataFrame:
    """
    Każda spółka startuje z równą częścią kapitału (equal-weight).
    Wartość pozycji w dniu t:
        V_t = (kapital / n) * (P_t / P_start)
    gdzie P_start = pierwszy dostępny kurs spółki.

    Zwraca DataFrame z kolumnami:
        - po jednej na każdą spółkę (wartość pozycji w USD)
        - 'Portfel_USD' – suma wszystkich pozycji
        - 'Portfel_pct' – stopa zwrotu portfela od startu [%]
    """
    kapital_na_spolke = kapital / len(portfolio)
    wyniki = {}

    for ticker in portfolio:
        col = pivot[ticker].dropna()
        if col.empty:
            continue
        p_start = col.iloc[0]
        wyniki[ticker] = kapital_na_spolke * (pivot[ticker] / p_start)

    df = pd.DataFrame(wyniki)
    df['Portfel_USD'] = df.sum(axis=1)
    df['Portfel_pct'] = (df['Portfel_USD'] / kapital - 1) * 100
    return df


# ── 3. rysunek ───────────────────────────────────────────────────────────────

def rysuj_portfel(df: pd.DataFrame, portfolio: dict, kapital: float) -> None:

    KOLORY = [
        '#2196F3', '#4CAF50', '#FF5722', '#9C27B0',
        '#E91E63', '#FF9800', '#009688', '#F44336',
        '#607D8B', '#00BCD4',
    ]

    fig, axes = plt.subplots(2, 1, figsize=(14, 10),
                             gridspec_kw={'height_ratios': [2, 1]},
                             sharex=True)
    fig.suptitle('Portfel NASDAQ Top 10 — wartość w czasie (equal-weight)',
                 fontsize=14, fontweight='bold', y=0.98)

    ax1, ax2 = axes

    # --- górny panel: wartości poszczególnych spółek + linia portfela -------
    tickers = list(portfolio.keys())
    for i, ticker in enumerate(tickers):
        if ticker not in df.columns:
            continue
        ax1.plot(df.index, df[ticker],
                 color=KOLORY[i % len(KOLORY)],
                 linewidth=1.0, alpha=0.65,
                 label=portfolio[ticker])

    ax1.plot(df.index, df['Portfel_USD'],
             color='black', linewidth=2.5,
             linestyle='--', label='Portfel (łącznie)', zorder=5)

    ax1.axhline(y=kapital, color='gray', linewidth=0.8,
                linestyle=':', alpha=0.6, label='Kapitał startowy')

    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f'${x:,.0f}'))
    ax1.set_ylabel('Wartość pozycji [USD]', fontsize=11)
    ax1.legend(ncol=3, fontsize=8, loc='upper left', framealpha=0.85)
    ax1.grid(axis='y', alpha=0.3, linewidth=0.5)
    ax1.grid(axis='x', alpha=0.2, linewidth=0.5)

    # wartość końcowa na wykresie
    kap_koniec = df['Portfel_USD'].dropna().iloc[-1]
    ax1.annotate(
        f'${kap_koniec:,.0f}',
        xy=(df['Portfel_USD'].dropna().index[-1], kap_koniec),
        xytext=(-60, 10), textcoords='offset points',
        fontsize=9, fontweight='bold', color='black',
        arrowprops=dict(arrowstyle='->', color='black', lw=1.2)
    )

    # --- dolny panel: stopa zwrotu portfela [%] ------------------------------
    ax2.fill_between(df.index, df['Portfel_pct'],
                     where=df['Portfel_pct'] >= 0,
                     color='#4CAF50', alpha=0.35, label='Zysk')
    ax2.fill_between(df.index, df['Portfel_pct'],
                     where=df['Portfel_pct'] < 0,
                     color='#F44336', alpha=0.35, label='Strata')
    ax2.plot(df.index, df['Portfel_pct'],
             color='black', linewidth=1.5)
    ax2.axhline(0, color='gray', linewidth=0.8, linestyle=':')

    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f'{x:+.0f}%'))
    ax2.set_ylabel('Stopa zwrotu [%]', fontsize=11)
    ax2.legend(fontsize=9, loc='upper left', framealpha=0.85)
    ax2.grid(axis='y', alpha=0.3, linewidth=0.5)
    ax2.grid(axis='x', alpha=0.2, linewidth=0.5)

    # oś X
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax2.set_xlabel('Data', fontsize=11)

    # podsumowanie w tytule dolnego panelu
    zwrot_total = df['Portfel_pct'].dropna().iloc[-1]
    ax2.set_title(
        f'Całkowity zwrot portfela: {zwrot_total:+.1f}%  '
        f'(${kapital:,.0f} → ${kap_koniec:,.0f})',
        fontsize=10, color='#333333'
    )

    plt.tight_layout()
    plt.savefig('portfel_wartosc_w_czasie.png', dpi=150, bbox_inches='tight')
    print("✅  Wykres zapisany jako 'portfel_wartosc_w_czasie.png'")
    plt.show()


# ── punkt wejścia ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 50)
    print("MODUŁ V – Wizualizacja wartości portfela")
    print("=" * 50)

    pivot     = wczytaj_kursy(DB_PATH, DB_TABLE, list(PORTFOLIO.keys()))
    df_portfel = oblicz_wartosc_portfela(pivot, PORTFOLIO, KAPITAL_USD)
    rysuj_portfel(df_portfel, PORTFOLIO, KAPITAL_USD)

 
# ============================================================
#  MODUŁ VI – Rebalancing portfela (roczny)
# ============================================================

def oblicz_portfel_rebalancing(pivot: pd.DataFrame, portfolio: dict, kapital: float) -> pd.DataFrame:
    """
    Symuluje portfel z corocznym rebalancingiem.
    Na koniec każdego roku (ostatni dzień sesyjny) kapitał jest ponownie 
    dzielony równo (po 10%) pomiędzy wszystkie spółki.
    """
    # 1. Pobieramy wspólny wycinek dat dla wszystkich spółek
    pivot_kompletny = pivot[list(portfolio.keys())].dropna()
    if pivot_kompletny.empty:
        raise ValueError("Brak wspólnych dat notowań dla wszystkich spółek portfela.")

    # 2. DEFINIOWANIE DAT REBALANCINGU
    # Grupujemy po roku i wyciągamy ostatni dzień (Date) z każdego roku
    dni_rebalancingu = pivot_kompletny.groupby(pivot_kompletny.index.year).apply(lambda x: x.index[-1]).values
    zbior_dat_rebalancingu = set(dni_rebalancingu) # Set dla szybszego wyszukiwania (O(1))

    waga_docelowa = 1.0 / len(portfolio)
    akcje = {} # Przechowuje "liczbę posiadanych akcji" dla każdego tickera
    historia = []

    # 3. Inicjalizacja portfela (Start: np. 2012-05-18)
    data_start = pivot_kompletny.index[0]
    ceny_start = pivot_kompletny.loc[data_start]
    
    kapital_poczatkowy_na_spolke = kapital * waga_docelowa
    for ticker in portfolio:
        # Kupujemy odpowiednią liczbę akcji (dopuszczamy ułamki dla dokładności matematycznej)
        akcje[ticker] = kapital_poczatkowy_na_spolke / ceny_start[ticker]

    # 4. Pętla po każdym dniu sesyjnym
    for data, ceny in pivot_kompletny.iterrows():
        # Oblicz bieżącą wartość portfela przed ewentualnym rebalancingiem
        wartosc_pozycji = {ticker: akcje[ticker] * ceny[ticker] for ticker in portfolio}
        calkowita_wartosc_portfela = sum(wartosc_pozycji.values())
        
        historia.append({
            'Date': data,
            'Wartość_Portfela_Rebal_USD': calkowita_wartosc_portfela
        })

        # ============================================================
        # TUTA JEST WARUNEK DOKONYWANIA REBALANCINGU
        # Jeśli bieżąca data to ostatni dzień sesyjny w danym roku 
        # (i nie jest to ostatni dzień całego badania, bo wtedy rebalancing nie ma sensu)
        # ============================================================
        if data in zbior_dat_rebalancingu and data != pivot_kompletny.index[-1]:
            
            # Nowy kapitał na spółkę = 10% bieżącej wartości całego portfela
            nowy_kapital_na_spolke = calkowita_wartosc_portfela * waga_docelowa
            
            # Realizujemy rebalancing - przeliczamy na nowo posiadaną liczbę akcji
            for ticker in portfolio:
                akcje[ticker] = nowy_kapital_na_spolke / ceny[ticker]

    df_historia = pd.DataFrame(historia).set_index('Date')
    
    # Obliczenie stopy zwrotu
    df_historia['Stopa_Zwrotu_Rebal_%'] = (df_historia['Wartość_Portfela_Rebal_USD'] / kapital - 1) * 100
    
    return df_historia


def tabela_porownawcza_rebalancing(df_rebal: pd.DataFrame, df_bh: pd.DataFrame, kapital: float) -> None:
    """Tworzy i drukuje tabelę podsumowującą stopę zwrotu za cały okres."""
    
    wartosc_koncowa_rebal = df_rebal['Wartość_Portfela_Rebal_USD'].iloc[-1]
    zwrot_rebal = df_rebal['Stopa_Zwrotu_Rebal_%'].iloc[-1]
    
    # Bierzemy dane końcowe z modułu Buy & Hold (Moduł V)[cite: 1]
    wartosc_koncowa_bh = df_bh['Portfel_USD'].iloc[-1]
    zwrot_bh = df_bh['Portfel_pct'].iloc[-1]
    
    wyniki = {
        'Strategia': ['Kup i Trzymaj (B&H)', 'Rebalancing Roczny (10%)'],
        'Kapitał Początkowy [USD]': [kapital, kapital],
        'Kapitał Końcowy [USD]': [wartosc_koncowa_bh, wartosc_koncowa_rebal],
        'Całkowita Stopa Zwrotu [%]': [zwrot_bh, zwrot_rebal]
    }
    
    df_wyniki = pd.DataFrame(wyniki)
    
    # Formatowanie wyświetlania
    formaty = {
        'Kapitał Początkowy [USD]': '${:,.2f}'.format,
        'Kapitał Końcowy [USD]': '${:,.2f}'.format,
        'Całkowita Stopa Zwrotu [%]': '{:,.2f}%'.format
    }
    for kolumna, funkcja in formaty.items():
        df_wyniki[kolumna] = df_wyniki[kolumna].apply(funkcja)
        
    print("=" * 70)
    print("MODUŁ VI – Porównanie strategii w całym okresie inwestycji")
    print("=" * 70)
    print(df_wyniki.to_string(index=False))

# ============================================================
# Wywołanie Modułu VI (dodaj pod wywołaniem Modułu V)
# ============================================================
print("\n" + "=" * 50)
print("MODUŁ VI – Symulacja rebalancingu")
print("=" * 50)

# 1. Obliczenie ścieżki portfela z rebalancingiem
df_rebal = oblicz_portfel_rebalancing(pivot, PORTFOLIO, KAPITAL_USD)

# 2. Wyświetlenie tabeli porównawczej (wykorzystuje df_portfel z Modułu V)
tabela_porownawcza_rebalancing(df_rebal, df_portfel, KAPITAL_USD)


# ============================================================
#  MODUŁ VII – Rebalancing portfela (zapalnik: VIX)
# ============================================================

def oblicz_portfel_vix(pivot: pd.DataFrame, portfolio: dict, kapital: float, prog_vix: float = 30.0, cooldown_dni: int = 20) -> pd.DataFrame:
    """
    Symuluje portfel z rebalancingiem aktywowanym wskaźnikiem VIX.
    Gdy VIX przekroczy zadany próg, kapitał jest ponownie dzielony równo (po 10%).
    Wprowadzono mechanizm 'cooldown_dni', aby zapobiec codziennemu rebalancingowi
    w okresach przedłużającej się rynkowej paniki.
    """
    
    # 1. Sprawdzenie, czy VIX został pobrany do pivotu
    if '^VIX' not in pivot.columns:
        raise ValueError("Brak tickera '^VIX' w danych! Upewnij się, że dodałeś VIX do zapytania SQL wczytującego pivot.")

    # 2. Wyodrębnienie danych (bierzemy pod uwagę tylko wspólne daty dla akcji)
    pivot_akcje = pivot[list(portfolio.keys())].dropna()
    vix_series = pivot['^VIX'].ffill() # Ffill wypełnia luki w notowaniach indeksu
    
    waga_docelowa = 1.0 / len(portfolio)
    akcje = {} 
    historia = []

    # 3. Inicjalizacja portfela
    data_start = pivot_akcje.index[0]
    ceny_start = pivot_akcje.loc[data_start]
    
    kapital_poczatkowy_na_spolke = kapital * waga_docelowa
    for ticker in portfolio:
        akcje[ticker] = kapital_poczatkowy_na_spolke / ceny_start[ticker]

    ostatni_rebalancing = data_start # Śledzimy datę ostatniego wyrównania

    # 4. Pętla sesja po sesji
    for data, ceny in pivot_akcje.iterrows():
        wartosc_pozycji = {ticker: akcje[ticker] * ceny[ticker] for ticker in portfolio}
        calkowita_wartosc_portfela = sum(wartosc_pozycji.values())
        
        historia.append({
            'Date': data,
            'Wartość_Portfela_VIX_USD': calkowita_wartosc_portfela,
            'Poziom_VIX': vix_series[data]
        })

        # ============================================================
        # TUTA JEST NOWY WARUNEK REBALANCINGU:
        # 1. VIX musi być większy lub równy założonemu progowi
        # 2. Od ostatniego rebalancingu musiało minąć więcej dni niż wynosi 'cooldown'
        # 3. Nie rebalansujemy w ostatnim dniu trwania badania
        # ============================================================
        # 2. Moduł VII: Wywołanie portfela opartego na VIX
        
        dni_od_rebalancingu = (data - ostatni_rebalancing).days
        biezacy_vix = vix_series.get(data, 0)
        
        if (biezacy_vix >= prog_vix) and (dni_od_rebalancingu >= cooldown_dni) and (data != pivot_akcje.index[-1]):
            
            nowy_kapital_na_spolke = calkowita_wartosc_portfela * waga_docelowa
            
            for ticker in portfolio:
                akcje[ticker] = nowy_kapital_na_spolke / ceny[ticker]
                
            ostatni_rebalancing = data # Zresetuj licznik cooldownu

    df_historia = pd.DataFrame(historia).set_index('Date')
    df_historia['Stopa_Zwrotu_VIX_%'] = (df_historia['Wartość_Portfela_VIX_USD'] / kapital - 1) * 100
    
    return df_historia

def tabela_porownawcza_vix_vs_roczny(df_vix: pd.DataFrame, df_roczny: pd.DataFrame, kapital: float, prog: float) -> None:
    """Tworzy tabelę wyników porównującą Rebalancing VIX z Rebalancingiem Rocznym."""
    wartosc_koncowa_vix = df_vix['Wartość_Portfela_VIX_USD'].iloc[-1]
    zwrot_vix = df_vix['Stopa_Zwrotu_VIX_%'].iloc[-1]
    
    # ZMIANA: Pobieramy dane z kolumn wygenerowanych przez Moduł VI
    wartosc_koncowa_roczny = df_roczny['Wartość_Portfela_Rebal_USD'].iloc[-1]
    zwrot_roczny = df_roczny['Stopa_Zwrotu_Rebal_%'].iloc[-1]
    
    wyniki = {
        'Strategia': ['Rebalancing Roczny (10%)', f'Rebalancing VIX (>{prog})'],
        'Kapitał Końcowy [USD]': [wartosc_koncowa_roczny, wartosc_koncowa_vix],
        'Całkowita Stopa Zwrotu [%]': [zwrot_roczny, zwrot_vix]
    }
    
    df_wyniki = pd.DataFrame(wyniki)
    for kolumna in ['Kapitał Końcowy [USD]']:
        df_wyniki[kolumna] = df_wyniki[kolumna].apply('${:,.2f}'.format)
    df_wyniki['Całkowita Stopa Zwrotu [%]'] = df_wyniki['Całkowita Stopa Zwrotu [%]'].apply('{:,.2f}%'.format)
        
    print("\n" + "=" * 65)
    print(f"MODUŁ VII – Porównanie: Rebalancing Roczny vs VIX > {prog}")
    print("=" * 65)
    print(df_wyniki.to_string(index=False))
    
    # ============================================================
#  PUNKT WEJŚCIA (WYWOŁANIE WSZYSTKICH MODUŁÓW)
# ============================================================

if __name__ == '__main__':
   
    # --- MODUŁ I --- (Zakładam, że baza jest już pobrana, więc można to zakomentować, 
    # aby nie pobierać danych z API za każdym uruchomieniem skryptu)
    # df_raw = pobierz_dane(WSZYSTKIE_TICKERY, DATA_OD, DATA_DO)
    # zapisz_do_bazy(df_raw, DB_PATH, DB_TABLE)
   
    # --- MODUŁ II ---
    # pivot = wczytaj_pivot(DB_PATH, DB_TABLE, list(PORTFOLIO.keys()))
    # df_wyniki = oblicz_portfel(pivot, PORTFOLIO, KAPITAL_USD)
    # drukuj_wyniki(df_wyniki)

    # ... (tutaj znajdują się wywołania Twoich modułów III, IV oraz VI jeśli je masz) ...

    # ============================================================
    #  MODUŁ V oraz VII (Wizualizacja B&H i Rebalancing VIX)
    # ============================================================
    print("\n" + "=" * 50)
    print("MODUŁ V i VII – Przygotowanie danych i symulacja")
    print("=" * 50)

    # Przygotowanie pełnej listy tickerów do pivotu (Akcje + VIX)
    tickery_do_analizy = list(PORTFOLIO.keys()) + ['^VIX']
    # Używamy funkcji wczytaj_kursy z Modułu V
    pivot_pelny = wczytaj_kursy(DB_PATH, DB_TABLE, tickery_do_analizy)

    # --- 1. Moduł V: Klasyczny portfel Buy & Hold (tylko akcje) ---
    print("\nObliczanie klasycznego portfela B&H (Moduł V)...")
    df_portfel = oblicz_wartosc_portfela(pivot_pelny, PORTFOLIO, KAPITAL_USD)
    # Rysowanie wykresu tymczasowo wyłączone dla szybkości testów tabeli (odkomentuj, gdy potrzebne)
    # rysuj_portfel(df_portfel, PORTFOLIO, KAPITAL_USD) 

    # --- 2. Moduł VII: Wywołanie portfela opartego na VIX ---
    print("Obliczanie portfela z rebalancingiem VIX (Moduł VII)...")
    PROG_VIX_TEST = 40.0
    COOLDOWN_DNI = 20
    
    df_vix_rebal = oblicz_portfel_vix(
        pivot_pelny, 
        PORTFOLIO, 
        KAPITAL_USD, 
        prog_vix=PROG_VIX_TEST, 
        cooldown_dni=COOLDOWN_DNI
    )
    
    # Wyświetlenie ostatecznego porównania
    tabela_porownawcza_vix(df_vix_rebal, df_portfel, KAPITAL_USD, PROG_VIX_TEST)

    # ============================================================
#  MODUŁ VIII – Wizualizacja porównawcza (B&H vs VIX Rebalancing)
# ============================================================

def rysuj_porownanie_portfeli_vix_vs_roczny(df_roczny: pd.DataFrame, df_vix: pd.DataFrame, prog_vix: float) -> None:
    """
    Rysuje wykres porównawczy: Rebalancing Roczny vs Rebalancing VIX.
    """
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), 
                             gridspec_kw={'height_ratios': [2, 1]}, 
                             sharex=True)
    fig.suptitle('Porównanie strategii: Rebalancing Roczny vs Rebalancing VIX', 
                 fontsize=14, fontweight='bold', y=0.98)

    ax1, ax2 = axes

    # --- Górny panel: Wartość kapitału ---
    # ZMIANA: df_roczny i kolumna Wartość_Portfela_Rebal_USD
    ax1.plot(df_roczny.index, df_roczny['Wartość_Portfela_Rebal_USD'], 
             color='#2196F3', linewidth=2, label='Rebalancing Roczny (10%)')
    ax1.plot(df_vix.index, df_vix['Wartość_Portfela_VIX_USD'], 
             color='#FF9800', linewidth=2, label=f'Rebalancing VIX (>= {prog_vix})')

    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax1.set_ylabel('Wartość portfela [USD]', fontsize=11)
    ax1.legend(loc='upper left', fontsize=10, framealpha=0.85)
    ax1.grid(axis='y', alpha=0.3)
    ax1.grid(axis='x', alpha=0.2)

    # --- Dolny panel: Obsunięcia kapitału (Drawdown) ---
    # ZMIANA: Obliczanie drawdownu z kolumn rocznego
    dd_roczny = (df_roczny['Wartość_Portfela_Rebal_USD'] / df_roczny['Wartość_Portfela_Rebal_USD'].cummax() - 1) * 100
    dd_vix = (df_vix['Wartość_Portfela_VIX_USD'] / df_vix['Wartość_Portfela_VIX_USD'].cummax() - 1) * 100

    ax2.plot(dd_roczny.index, dd_roczny, color='#2196F3', linewidth=1.2, alpha=0.8, label='Drawdown Roczny')
    ax2.plot(dd_vix.index, dd_vix, color='#FF9800', linewidth=1.2, alpha=0.8, label='Drawdown VIX')
    
    ax2.fill_between(dd_roczny.index, dd_roczny, 0, color='#2196F3', alpha=0.1)
    ax2.fill_between(dd_vix.index, dd_vix, 0, color='#FF9800', alpha=0.15)

    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
    ax2.set_ylabel('Obsunięcie kapitału [%]', fontsize=11)
    ax2.legend(loc='lower left', fontsize=10, framealpha=0.85)
    ax2.grid(axis='y', alpha=0.3)
    ax2.grid(axis='x', alpha=0.2)

    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax2.set_xlabel('Data', fontsize=11)

    plt.tight_layout()
    plt.savefig('porownanie_rebalancingow.png', dpi=150, bbox_inches='tight')
    print("✅  Wykres porównawczy zapisany jako 'porownanie_rebalancingow.png'")
    plt.show()

    # ============================================================
#  PUNKT WEJŚCIA
# ============================================================

if __name__ == '__main__':
    
    print("\n" + "=" * 50)
    print("URUCHAMIANIE GŁÓWNEGO PROCESU: Roczny vs VIX")
    print("=" * 50)

    # 1. Wczytanie danych z bazy dla portfela i VIX
    print("Wczytywanie danych z bazy...")
    tickery_do_analizy = list(PORTFOLIO.keys()) + ['^VIX']
    pivot_pelny = wczytaj_kursy(DB_PATH, DB_TABLE, tickery_do_analizy)

    # 2. Obliczenia dla Rebalancingu Rocznego (Moduł VI)
    # Wywołujemy funkcję z modułu VI, żeby uzyskać df_rebal_roczny
    print("Obliczanie portfela z rebalancingiem rocznym...")
    df_rebal_roczny = oblicz_portfel_rebalancing(pivot_pelny, PORTFOLIO, KAPITAL_USD)

    # 3. Obliczenia dla rebalancingu VIX (Moduł VII)
    print("Obliczanie portfela z rebalancingiem VIX...")
    PROG_VIX_TEST = 30.0
    COOLDOWN_DNI = 20
    df_vix_rebal = oblicz_portfel_vix(pivot_pelny, PORTFOLIO, KAPITAL_USD, prog_vix=PROG_VIX_TEST, cooldown_dni=COOLDOWN_DNI)
    
    # 4. Tabela porównawcza obu strategii
    tabela_porownawcza_vix_vs_roczny(df_vix_rebal, df_rebal_roczny, KAPITAL_USD, PROG_VIX_TEST)

    # 5. Ostateczna wizualizacja (Moduł VIII)
    print("Generowanie wykresu porównawczego...")
    rysuj_porownanie_portfeli_vix_vs_roczny(df_rebal_roczny, df_vix_rebal, PROG_VIX_TEST)

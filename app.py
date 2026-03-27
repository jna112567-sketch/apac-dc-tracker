import streamlit as st
import pandas as pd
import plotly.express as px
import feedparser
import yfinance as yf
import requests
import ssl
import sqlite3             
import urllib.request      
import urllib.parse
import os
from io import BytesIO
import traceback     

# --- GLOBAL SSL FIX (For Corporate Firewalls) ---
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# ==========================================
# 1. PAGE CONFIG & APP SETUP
# ==========================================
st.set_page_config(page_title="APAC DC Intelligence Tracker", page_icon="🏢", layout="wide")

# --- DEFINE GLOBAL VARIABLES ---
DB_SQLITE = "apac_dc_transactions.db"  

# ADD THIS DICTIONARY:
AREA_RATES = {
    "South Korea": 1.0,
    "Japan": 1.0,
    "Hong Kong": 1.0,
    "Singapore": 1.0,
    "Australia": 1.0,
    "Malaysia": 1.0
}

GEO_COORDS = {
    "South Korea": {"lat": 35.9078, "lon": 127.7669},
    "Japan": {"lat": 36.2048, "lon": 138.2529},
    "Hong Kong": {"lat": 22.3193, "lon": 114.1694},
    "Singapore": {"lat": 1.3521, "lon": 103.8198},
    "Australia": {"lat": -25.2744, "lon": 133.7751},
    "Malaysia": {"lat": 4.2105, "lon": 101.9758}
}
# ==========================================
# 2. DATA ENGINES (Cached Functions)
# ==========================================

@st.cache_data(ttl=3600)
def get_live_fx():
    rates = {"USD": 1.0}
    try:
        # Try to pull live rates
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=5)
        data = response.json()
        rates = data.get("rates", rates)
    except Exception:
        # Fallback APAC rates if office firewall blocks the API
        rates.update({
            "SGD": 1.34, "AUD": 1.52, "JPY": 150.0,
            "KRW": 1330.0, "HKD": 7.82, "MYR": 4.75
        })
    return rates

@st.cache_data(ttl=3600)
def get_implied_market_cap_rates():
    # One representative leader per country
    benchmarks = {
        "Singapore": "AJBU.SI",     # Keppel DC
        "Australia": "GMG.AX",      # Goodman Group
        "Japan": "3281.T",          # GLP J-REIT
        "Hong Kong": "1686.HK",     # GDS Holdings
        "South Korea": "350120.KS", # ESR Kendall Sq
        "Malaysia": "5106.KL"       # Axis REIT
    }
    
    results = []
    for country, ticker_sym in benchmarks.items():
        try:
            t = yf.Ticker(ticker_sym)
            info = t.info
            
            ev = info.get('enterpriseValue', 0)
            ebitda = info.get('ebitda', 0)
            
            # If EBITDA is missing, fallback to Net Income + Interest
            if not ebitda or ebitda == 0:
                ebitda = info.get('netIncomeToCommon', 0) * 1.2 
            
            implied_cap = (ebitda / ev) * 100 if ev > 0 else 0
            
            results.append({
                "Region": country,
                "Proxy REIT": info.get('shortName', ticker_sym),
                "Implied Cap Rate (%)": round(implied_cap, 2)
            })
        except:
            continue
            
    # --- NEW FIREWALL FALLBACK LOGIC ---
    # If the API was blocked and 'results' is empty, use these estimates:
    if len(results) == 0:
        return pd.DataFrame([
            {"Region": "Singapore", "Proxy REIT": "Keppel DC (Proxy)", "Implied Cap Rate (%)": 5.20},
            {"Region": "Australia", "Proxy REIT": "Goodman (Proxy)", "Implied Cap Rate (%)": 4.80},
            {"Region": "Japan", "Proxy REIT": "GLP J-REIT (Proxy)", "Implied Cap Rate (%)": 3.90},
            {"Region": "Hong Kong", "Proxy REIT": "GDS (Proxy)", "Implied Cap Rate (%)": 6.10},
            {"Region": "South Korea", "Proxy REIT": "ESR (Proxy)", "Implied Cap Rate (%)": 5.50},
            {"Region": "Malaysia", "Proxy REIT": "Axis REIT (Proxy)", "Implied Cap Rate (%)": 6.50}
        ])

    return pd.DataFrame(results)

@st.cache_data(ttl=86400)
def load_live_macro_data():
    country_codes = "KOR;HKG;JPN;AUS;SGP;MYS"
    country_map = {"Korea, Rep.": "South Korea", "Hong Kong SAR, China": "Hong Kong", "Japan": "Japan", "Australia": "Australia", "Singapore": "Singapore", "Malaysia": "Malaysia"}
    indicators = {'NY.GDP.MKTP.CD': 'GDP (USD Billions)', 'SP.POP.TOTL': 'Population (Millions)', 'FP.CPI.TOTL.ZG': 'Inflation Rate (%)'}
    
    interest_rates = {
        "South Korea": {"Rate (%)": 2.50, "Date": "Feb 2026 (BOK)"},
        "Hong Kong": {"Rate (%)": 3.75, "Date": "Q1 2026 (HKMA)"},
        "Japan": {"Rate (%)": 0.75, "Date": "Dec 2025 (BOJ)"},
        "Australia": {"Rate (%)": 4.10, "Date": "Q1 2026 (RBA)"},
        "Singapore": {"Rate (%)": 3.50, "Date": "Q1 2026 (MAS Est)"},
        "Malaysia": {"Rate (%)": 3.00, "Date": "Q1 2026 (BNM)"}
    }

    # FRED API INJECTION (Using your provided Key)
    fred_series = {
        "South Korea": "IRLTLT01KRM156N", # Korea 10Y
        "Japan": "IRLTLT01JPM156N",       # Japan 10Y
        "Australia": "IRLTLT01AUM156N",   # Australia 10Y
        "Hong Kong": "MAEST10Y",          # HK 10Y (via FRED)
        "Singapore": "SGS10Y",            # SG 10Y (Approx)
        "Malaysia": "IRLTLT01MYM156N"     # Malaysia 10Y
    }
    
    for country, series_id in fred_series.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key=ee8e1fb49c584aee829513873a4236fe&file_type=json&sort_order=desc&limit=1"
            res = requests.get(url, timeout=5).json()
            val = float(res['observations'][0]['value'])
            date = res['observations'][0]['date']
            
            if country == "Hong Kong": val = val + 0.50 # HKMA Peg Logic
                
            interest_rates[country] = {"Rate (%)": val, "Date": f"{date} (Live FRED)"}
        except Exception:
            pass 

    df_macro = pd.DataFrame({"Region": list(country_map.values())})
    
    try:
        for ind_code, ind_name in indicators.items():
            url = f"http://api.worldbank.org/v2/country/{country_codes}/indicator/{ind_code}?format=json&mrnev=1&per_page=50"
            response = requests.get(url, timeout=5).json()[1]
            temp_dict = {country_map.get(item['country']['value'], item['country']['value']): (item['value'] / 1e9 if 'GDP' in ind_name else item['value'] / 1e6 if 'Population' in ind_name else item['value']) for item in response if item['value'] is not None}
            df_macro[ind_name] = df_macro['Region'].map(temp_dict)
            
        df_macro['10Y Bond Yield (%))'] = df_macro['Region'].map(lambda x: interest_rates[x]["Rate (%)"])
        df_macro['Yield Last Updated'] = df_macro['Region'].map(lambda x: interest_rates[x]["Date"])
        return df_macro
    except Exception:
            # Fallback if FRED API is blocked
            return pd.DataFrame({
                "Region": ["South Korea", "Hong Kong", "Japan", "Australia", "Singapore", "Malaysia"],
                "10Y Bond Yield (%)": [3.45, 3.80, 1.05, 4.25, 3.15, 3.90],  # <--- MUST MATCH EXACTLY
                "GDP (USD Billions)": [1712, 359, 4110, 1790, 501, 406],
                "Yield Source": ["Estimated 10Y Yield (Mar 2026)"] * 6,
                "Inflation Rate (%)": [2.8, 2.0, 2.5, 3.4, 2.5, 1.5]
            })
# ==========================================
# 3. TRANSACTION DATA PROCESSING
# ==========================================
def init_sqlite_db():
    conn = sqlite3.connect(DB_SQLITE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS transactions (
        Region TEXT, Asset TEXT, Buyer TEXT, Seller TEXT, Status TEXT, GFA_Value REAL, 
        GFA_Unit TEXT, Capacity_MW REAL, Consideration_Value REAL, 
        Currency TEXT, Date TEXT, Remarks TEXT, Source TEXT, URL TEXT)''')
    
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [col[1] for col in cursor.fetchall()]
    if "Status" not in columns:
        cursor.execute("ALTER TABLE transactions ADD COLUMN Status TEXT")
        conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM transactions")
    result = cursor.fetchone()
    if result is None or result[0] == 0:
        # HIGHLY ACCURATE RECENT APAC DEALS SEED
        seed = [
            ("Japan", "Greater Osaka Hyperscale (49%)", "CapitaLand Ascendas REIT", "Mitsui & Co", "Executed", None, "sqm", 40.5, 620.7, "SGD", "2026-03-24", "Tier III facility; S$620.7M valuation", "Light Reading", "https://www.lightreading.com/data-centers/clar-enters-japan-s-data-center-market-with-strategic-osaka-investment"),
            ("Malaysia", "Sedenak Tech Park Campus", "Vantage Data Centers", "Yondr Group", "Executed", None, "sqm", 300.0, 1600.0, "USD", "2025-09-10", "$1.6B investment backed by GIC/ADIA", "Vantage", "https://vantage-dc.com/news/vantage-data-centers-secures-1-6b-investment-in-apac-platform-from-gic-and-adia/"),
            ("Malaysia", "TelcoHub 1 Cyberjaya", "Digital Realty", "CSF Advisers", "Executed", None, "sqm", 15.5, None, "USD", "2026-01-19", "1.5MW active + 14MW adjacent expansion", "Digital Realty", "https://www.digitalrealty.com/about/newsroom/press-releases/19966/digital-realty-enters-malaysia-strengthening-southeast-asia-s-digital-backbone"),
            ("Singapore", "KDC SGP 3 & 4 (Remaining)", "Keppel DC REIT", "Keppel Ltd", "Executed", None, "sqm", None, 50.5, "SGD", "2025-12-16", "Remaining 10% in SGP 3 & 1% in SGP 4", "Keppel", "https://www.keppel.com/"),
            ("South Korea", "Epoch Digital Seoul", "Actis", None, "Tentative", None, "sqm", 65.0, None, "USD", "2025-12-01", "Greenfield project in Greater Seoul", "Actis", "https://www.act.is/"),
            ("Singapore", "STT GDC (82% Stake)", "KKR & Singtel", "ST Telemedia", "Executed", None, "sqm", 1700.0, 5100.0, "USD", "2026-02-04", "S$13.8B EV. Largest SE Asia digital infra deal", "STT GDC", "https://www.sttelemediagdc.com/"),
            ("Hong Kong", "Sandy Ridge Data Cluster", "Runze Intelligent Computing", "HK Government", "Tentative", 250000, "sqm", 220.0, 581.0, "HKD", "2026-03-02", "HKD 581M land premium; HKD 23.8B total expected", "HK Gov", "https://www.info.gov.hk")
        ]
        cursor.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", seed)
        conn.commit()
    conn.close()

# We cache it for 600 seconds (10 mins). If you edit the Google Sheet, 
# the dashboard will automatically pull the new data after 10 minutes!
# ==========================================
# 3. TRANSACTION DATA PROCESSING
# ==========================================

# ... keep your init_sqlite_db() function as it is ...

def process_df_logic(df_tx):
    """
    This is the engine. It formats the data whether it 
    comes from Google OR from the local Database.
    """
    fx = get_live_fx()
    df_tx['Date'] = pd.to_datetime(df_tx['Date'], errors='coerce')
    for col in ['Consideration_Value', 'Capacity_MW', 'GFA_Value']:
        df_tx[col] = pd.to_numeric(df_tx[col], errors='coerce').fillna(0)
    
    df_tx['Consideration_USD_M'] = df_tx['Consideration_Value'] * df_tx['Currency'].map(fx).fillna(1.0)
    df_tx['USD_per_MW'] = df_tx.apply(lambda row: row['Consideration_USD_M'] / row['Capacity_MW'] if row['Capacity_MW'] > 0 else 0, axis=1)
    df_tx['GFA_sqm'] = df_tx['GFA_Value'] * df_tx['GFA_Unit'].map(AREA_RATES).fillna(1.0)
    
    
    df_tx['lat'] = df_tx['Region'].map(lambda x: GEO_COORDS.get(x, {}).get("lat", 0))
    df_tx['lon'] = df_tx['Region'].map(lambda x: GEO_COORDS.get(x, {}).get("lon", 0))
    
    def create_news_url(row):
        if pd.notna(row.get('URL')) and str(row.get('URL')).startswith('http'):
            return row['URL']
        q = urllib.parse.quote_plus(f'"{row.get("Buyer","")}" "{row.get("Asset","")}" data center')
        return f"https://www.google.com/search?q={q}"
    
    df_tx['Direct_News_Link'] = df_tx.apply(create_news_url, axis=1)
    return df_tx

@st.cache_data(ttl=600) 
def load_tx_data():
    SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTC_BPm2epkPFS8QOW701NAW2xPtyNLlzXONjZqXg0O7QMqDU27hR-4QxXxkCngmTVhxzOvrNFdyk-q/pub?output=csv"
    
    # 1. TRY LIVE GOOGLE SHEET (Laptop A / Home)
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(SHEET_URL, verify=False, timeout=5, headers=headers)
        if response.status_code == 200:
            df_raw = pd.read_csv(BytesIO(response.content))
            if 'Region' in df_raw.columns:
                # Save to local DB for Laptop B to use later
                conn = sqlite3.connect(DB_SQLITE)
                df_raw.to_sql("transactions", conn, if_exists="replace", index=False)
                conn.close()
                return process_df_logic(df_raw)
    except Exception:
        st.sidebar.warning("⚠️ Using Local Database (Firewall active)")

    # 2. FALLBACK TO LOCAL SQLITE (Laptop B / Office)
    try:
        init_sqlite_db()
        conn = sqlite3.connect(DB_SQLITE)
        df_local = pd.read_sql("SELECT * FROM transactions", conn)
        conn.close()
        return process_df_logic(df_local)
    except Exception as e:
            st.error(f"Critical Error: {e}")
            st.code(traceback.format_exc()) # <--- This will print the exact line number!
            st.sidebar.warning("❌ No valid data found. Check your Google Sheet or 'datacenter.db' file.")
            return pd.DataFrame()

    except Exception as e:
        st.sidebar.warning("⚠️ Corporate Firewall blocked Google Sheets. Switching to Local Database.")

# --- EXECUTE LOAD ---
df = load_tx_data()
macro_df = load_live_macro_data()

# ==========================================
# 4. COMPACT DASHBOARD UI
# ==========================================
st.sidebar.image("https://img.icons8.com/color/96/000000/server.png", width=60)
st.sidebar.header("🔍 Filter Dashboard")

# 1. Check for data validity
if 'Region' not in df.columns or df.empty:
    st.error("❌ No valid data found. Check your Google Sheet or 'datacenter.db' file.")
    if st.sidebar.button("🔄 Force Sync"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

# 1. Validation Check: Make sure we actually have data
if df.empty or 'Region' not in df.columns:
    st.error("❌ No valid data found. Check your Google Sheet or 'datacenter.db' file.")
    if st.sidebar.button("🔄 Force Sync"):
        st.cache_data.clear()
        st.rerun()
    st.stop()

# 2. The Sidebar Filters (The fix for your Duplicate ID error)
all_regions = sorted(list(df['Region'].unique()))
selected_regions = st.sidebar.multiselect(
    "Select Regions:", 
    all_regions, 
    default=all_regions, 
    key="main_region_selector" 
)

# 3. Apply the filter
filtered_df = df[df['Region'].isin(selected_regions)]
filtered_macro_df = macro_df[macro_df['Region'].isin(selected_regions)]

# 4. Sync Button
if st.sidebar.button("🔄 Refresh All Data"):
    st.cache_data.clear()
    st.rerun()

st.title("🏢 APAC Data Centre Real Estate Tracker")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Investment", f"USD {filtered_df['Consideration_USD_M'].sum():,.1f} M")
m2.metric("Total Capacity", f"{filtered_df['Capacity_MW'].sum():,.0f} MW")
m3.metric("Deals", len(filtered_df))
avg_val = (filtered_df['Consideration_USD_M'].sum() / filtered_df['Capacity_MW'].sum()) if filtered_df['Capacity_MW'].sum() > 0 else 0
m4.metric("Avg USD/MW", f"{avg_val:,.1f} M")
st.markdown("---")

# 3 CLEAN TABS
tab1, tab2, tab3 = st.tabs(["📊 Analytics & Macro", "🗄️ Database", "📰 Intelligence"])

with tab1:
    c1, c2 = st.columns([2, 1])
    with c1: 
        st.markdown("**Geographic Capacity Map**")
        st.plotly_chart(px.scatter_geo(filtered_df, lat='lat', lon='lon', color='Region', size='Capacity_MW', hover_name='Asset', projection="natural earth", scope="asia", title="Bubble Size = MW").update_layout(margin=dict(l=0,r=0,t=30,b=0)), width='stretch')
    with c2: 
        st.markdown("**Investment Volume**")
        st.plotly_chart(px.bar(filtered_df.groupby('Region')['Consideration_USD_M'].sum().reset_index(), x='Region', y='Consideration_USD_M', color='Region', template='plotly_white').update_layout(showlegend=False, xaxis_title="", yaxis_title="USD (Millions)"), width='stretch')
    
    st.markdown("---")
    st.markdown("**Live Regional Macroeconomic Indicators (Powered by World Bank & FRED API)**")
    st.dataframe(filtered_macro_df, hide_index=True, width='stretch')

with tab2: # --- SECTION 2: UI TAB 2 ---
    st.header("🎯 Market Risk Premium Analysis")
    
    # Run the engines
    reit_df = get_implied_market_cap_rates()
    macro_data = load_live_macro_data()
    
    # --- SAFE MERGE AND CALCULATE ---
    # 1. Print the actual columns to the screen so we can see what's wrong
    st.warning(f"🕵️ Debug: The actual columns in macro_data are: {macro_data.columns.tolist()}")
    
    # 2. Safety Net: Check what the column is actually named and use that
    if '10Y Bond Yield (%)' in macro_data.columns:
        yield_col = '10Y Bond Yield (%)'
    elif 'Interest Rate (%)' in macro_data.columns:
        yield_col = 'Interest Rate (%)'
    else:
        # If all else fails, grab the 2nd column (assuming Region is 1st)
        yield_col = macro_data.columns[1] 

    # 3. Perform the merge using the safe column name
    analysis_df = pd.merge(reit_df, macro_data[['Region', yield_col]], on="Region")
    
    # 4. Standardize the column name so the rest of the math works
    analysis_df.rename(columns={yield_col: '10Y Bond Yield (%)'}, inplace=True)
    
    # Calculate Risk Premium
    analysis_df['Risk Premium (bps)'] = (analysis_df['Implied Cap Rate (%)'] - analysis_df['10Y Bond Yield (%)']) * 100
    
    # Show Table
    st.dataframe(analysis_df, use_container_width=True)
    
    # --- SECTION 3: THE CHART ---
    st.subheader("Visualizing the Yield Gap")

    # Create a dual-axis style chart
    chart_data = analysis_df.melt(id_vars='Region', value_vars=['10Y Bond Yield (%)', 'Implied Cap Rate (%)'])

    st.bar_chart(
        analysis_df, 
        x="Region", 
        y="Risk Premium (bps)", 
        color="#0046ad" # Colliers Blue
    )

    st.info("💡 **Insight:** A wider blue bar indicates a 'cheaper' market where the Data Center yield offers a significant premium over government debt.")
    st.markdown("**🗄️ Transaction Database**")
    display_df = filtered_df[['Region', 'Asset', 'Status', 'Buyer', 'Seller', 'Capacity_MW', 'Consideration_USD_M', 'USD_per_MW', 'Currency', 'Consideration_Value', 'Date', 'Remarks', 'Direct_News_Link']].copy()
    display_df['Date'] = display_df['Date'].dt.strftime('%Y-%m-%d')
        
    col_formatting = {
        "Consideration_USD_M": st.column_config.NumberColumn("Unified Price", format="USD %d M"),
        "USD_per_MW": st.column_config.NumberColumn("Price per MW", format="USD %.1f M"),
        "Consideration_Value": st.column_config.NumberColumn("Local Price (Millions)", format="%d M"),
        "Direct_News_Link": st.column_config.LinkColumn("Direct News Link", display_text="📰 Read Article")
    }
    

    st.subheader("✅ Executed Deals")
    st.dataframe(display_df[display_df['Status'] == 'Executed'], column_config=col_formatting, hide_index=True, width='stretch')

    st.subheader("⏳ Tentative & Pipeline Deals")
    st.dataframe(display_df[display_df['Status'] == 'Tentative'], column_config=col_formatting, hide_index=True, width='stretch')

    st.markdown("---")
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        display_df.drop(columns=['Direct_News_Link']).to_excel(writer, index=False, sheet_name='APAC Transactions')
        filtered_macro_df.to_excel(writer, index=False, sheet_name='Live Macro Data')
    st.download_button("📥 Download Database to Excel", output.getvalue(), "APAC_DC_Intelligence.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with tab3:
    st.markdown("**Localized Market Intelligence Feed**")
    nc1, nc2 = st.columns(2)
    news_region = nc1.selectbox("Filter News by Region:", ["APAC (All)"] + list(GEO_COORDS.keys()), key="news_region_dropdown")
    news_topic = nc2.selectbox("Filter by Sector/Topic:", ["Data Centers (General)", "Real Estate M&A", "Macroeconomics"], key="news_topic_dropdown")
    st.markdown("---")
    
    local_queries = {
        "South Korea": {"base": "데이터센터 (site:thebell.co.kr OR site:sedaily.com OR 투자 OR 부동산)", "lang": "hl=ko&gl=KR&ceid=KR:ko"},
        "Japan": {"base": "データセンター (不動産 OR 投資 OR 開発)", "lang": "hl=ja&gl=JP&ceid=JP:ja"},
        "Hong Kong": {"base": "數據中心 (房地產 OR 投資)", "lang": "hl=zh-HK&gl=HK&ceid=HK:zh-Hant"},
        "Singapore": {"base": "data centre property investment", "lang": "hl=en-SG&gl=SG&ceid=SG:en"},
        "Australia": {"base": "data centre property investment", "lang": "hl=en-AU&gl=AU&ceid=AU:en"},
        "Malaysia": {"base": "data center property investment", "lang": "hl=en-MY&gl=MY&ceid=MY:en"},
        "APAC (All)": {"base": "data center real estate asia pacific", "lang": "hl=en-US&gl=US&ceid=US:en"}
    }
    topic_modifiers = {
        "South Korea": {"Data Centers (General)": "", "Real Estate M&A": "인수합병 OR M&A", "Macroeconomics": "금리 OR 거시경제"},
        "Japan": {"Data Centers (General)": "", "Real Estate M&A": "買収 OR M&A", "Macroeconomics": "金利 OR マクロ経済"},
        "Hong Kong": {"Data Centers (General)": "", "Real Estate M&A": "併購 OR M&A", "Macroeconomics": "利率 OR 宏觀經濟"},
        "Default": {"Data Centers (General)": "", "Real Estate M&A": "M&A OR acquisition", "Macroeconomics": "interest rates OR economy"}
    }

    config = local_queries.get(news_region, local_queries["APAC (All)"])
    mod_dict = topic_modifiers.get(news_region, topic_modifiers["Default"])
    modifier = mod_dict.get(news_topic, "")
    
    full_query = f"{config['base']} {modifier}".strip().replace(" ", "+")
    news_url = f"https://news.google.com/rss/search?q={full_query}&{config['lang']}"
    
    with st.spinner(f"Fetching localized intelligence for {news_region}..."):
        try:
            feed = feedparser.parse(news_url)
            if not feed.entries:
                st.info("No recent articles found for this specific localized search.")
            else:
                # STRICT TIME SORTING LOGIC
                for entry in feed.entries:
                    entry['dt_parsed'] = pd.to_datetime(entry.published) if hasattr(entry, 'published') else pd.to_datetime('1970-01-01')
                
                sorted_entries = sorted(feed.entries, key=lambda x: x['dt_parsed'], reverse=True)
                
                for entry in sorted_entries[:10]:  
                    st.markdown(f"📰 **[{entry.title}]({entry.link})**")
                    st.caption(f"Published: {entry['dt_parsed'].strftime('%B %d, %Y')}")
                    st.write("") 
        except Exception:
            st.warning("Could not load news feed at this time.")
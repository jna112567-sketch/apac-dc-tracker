import streamlit as st
import pandas as pd
import plotly.express as px
import feedparser
from io import BytesIO
import urllib.parse
import os
import yfinance as yf
import requests
import sqlite3

# ==========================================
# 1. PAGE CONFIG & APP SETUP
# ==========================================
st.set_page_config(page_title="APAC Data Centre Tracker", layout="wide", page_icon="🏢")

GEO_COORDS = {
    "South Korea": {"lat": 35.9078, "lon": 127.7669},
    "Hong Kong": {"lat": 22.3193, "lon": 114.1694},
    "Australia": {"lat": -25.2744, "lon": 133.7751},
    "Malaysia": {"lat": 4.2105, "lon": 101.9758},
    "Singapore": {"lat": 1.3521, "lon": 103.8198},
    "Japan": {"lat": 36.2048, "lon": 138.2529}
}

AREA_RATES = {"sqm": 1.0, "sqft": 0.0929, "acres": 4046.86}
DB_FILE = "database.xlsx"

EXPECTED_TX_COLUMNS = [
    "Region", "Asset", "Buyer", "Seller", "GFA_Value", "GFA_Unit", 
    "Capacity_MW", "Consideration_Value", "Currency", "Date", "Remarks", "Source", "URL"
]

# ==========================================
# 2. LIVE API DATA FETCHERS
# ==========================================
@st.cache_data(ttl=86400) # Cache for 24 hours to keep the app lightning fast
def get_live_fx():
    """Pulls live exchange rates using yfinance"""
    rates = {"USD": 1.0}
    tickers = {"KRW": "KRW=X", "HKD": "HKD=X", "AUD": "AUDUSD=X", "SGD": "SGD=X", "JPY": "JPY=X"}
    
    for currency, ticker in tickers.items():
        try:
            # Fetch the latest closing price
            data = yf.Ticker(ticker).history(period="1d")
            rate = data['Close'].iloc[-1]
            # AUDUSD is inverted, the rest are USD to Local
            if currency == "AUD":
                rates[currency] = rate 
            else:
                rates[currency] = 1 / rate
        except Exception:
            # Fallback if Yahoo Finance is down
            fallback = {"KRW": 0.00074, "HKD": 0.13, "AUD": 0.65, "SGD": 0.74, "JPY": 0.0066}
            rates[currency] = fallback[currency]
    return rates

@st.cache_data(ttl=86400) # Cache for 24 hours
def load_live_macro_data():
    """Pulls live macro data directly from the World Bank API using requests"""
    country_map = {"KOR": "South Korea", "HKG": "Hong Kong", "JPN": "Japan", 
                   "AUS": "Australia", "SGP": "Singapore", "MYS": "Malaysia"}
    
    indicators = {
        'NY.GDP.MKTP.CD': 'GDP (USD Billions)', 
        'SP.POP.TOTL': 'Population (Millions)',
        'FP.CPI.TOTL.ZG': 'Inflation Rate (%)'
    }
    
    data_rows = []
    
    try:
        # Loop through each country and fetch the most recent data
        for c_code, c_name in country_map.items():
            row_data = {"Region": c_name}
            
            for ind_code, ind_name in indicators.items():
                try:
                    # mrnev=1 tells the World Bank to give us the "Most Recent Non-Empty Value"
                    url = f"http://api.worldbank.org/v2/country/{c_code}/indicator/{ind_code}?format=json&mrnev=1"
                    response = requests.get(url, timeout=5).json()
                    
                    # Extract the actual number from the JSON response
                    val = response[1][0]['value']
                    
                    # Format the numbers for the dashboard
                    if val is not None:
                        if 'GDP' in ind_name:
                            val = val / 1_000_000_000
                        elif 'Population' in ind_name:
                            val = val / 1_000_000
                            
                    row_data[ind_name] = val
                except Exception:
                    row_data[ind_name] = None
                    
            data_rows.append(row_data)
            
        return pd.DataFrame(data_rows)
        
    except Exception as e:
        st.warning("Could not reach World Bank API. Using cached fallback data.")
        # Fallback data just in case you are completely offline
        return pd.DataFrame({
            "Region": ["South Korea", "Hong Kong", "Japan", "Australia", "Singapore", "Malaysia"],
            "GDP (USD Billions)": [1712, 359, 4110, 1790, 501, 406],
            "Inflation Rate (%)": [2.8, 2.0, 2.5, 3.4, 2.5, 1.5],
            "Population (Millions)": [51.4, 7.5, 124.5, 26.6, 5.9, 34.3]
        })

# ==========================================
# 3. TRANSACTION DATA LOADING
# ==========================================
DB_SQLITE = "datacenter.db"

def init_sqlite_db():
    """Creates the database and table if they don't exist, and adds sample data."""
    conn = sqlite3.connect(DB_SQLITE)
    cursor = conn.cursor()
    # Create the table using the columns we need
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            Region TEXT, Asset TEXT, Buyer TEXT, Seller TEXT, 
            GFA_Value REAL, GFA_Unit TEXT, Capacity_MW REAL, 
            Consideration_Value REAL, Currency TEXT, Date TEXT, 
            Remarks TEXT, Source TEXT, URL TEXT
        )
    ''')
    
    # Check if it's empty; if so, add the seed data
    cursor.execute("SELECT COUNT(*) FROM transactions")
    if cursor.fetchone()[0] == 0:
        seed_data = [
            ("South Korea", "Ansan Hyperscale", "Macquarie", "Gabia", 450000, "sqft", 100, 600, "KRW", "2026-03-03", "Strategic AI Hub", "Macquarie", "https://example.com"),
            ("Hong Kong", "Fanling DCs", "Actis", "Grand Ming", 17187, "sqm", 16, 5250, "HKD", "2026-10-21", "Advanced Talks", "Mingtiandi", "https://example.com")
        ]
        cursor.executemany('''
            INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', seed_data)
        conn.commit()
    conn.close()

@st.cache_data
def load_tx_data():
    """Loads data from SQLite, converts to DataFrame, and performs live FX/Calculations"""
    init_sqlite_db() # Ensure DB exists
    
    conn = sqlite3.connect(DB_SQLITE)
    df_tx = pd.read_sql("SELECT * FROM transactions", conn)
    conn.close()

    # --- THE REST OF YOUR LOGIC (Live FX & Formatting) ---
    live_fx = get_live_fx()
    df_tx['Date'] = pd.to_datetime(df_tx['Date'], errors='coerce')
    
    for col in ['Consideration_Value', 'Capacity_MW', 'GFA_Value']:
        df_tx[col] = pd.to_numeric(df_tx[col], errors='coerce').fillna(0)
    
    # Calculate using live exchange rates
    df_tx['Consideration_USD_M'] = df_tx['Consideration_Value'] * df_tx['Currency'].map(live_fx).fillna(1.0)
    df_tx['GFA_sqm'] = df_tx['GFA_Value'] * df_tx['GFA_Unit'].map(AREA_RATES).fillna(1.0)
    
    # Map coordinates
    df_tx['lat'] = df_tx['Region'].map(lambda x: GEO_COORDS.get(x, {}).get("lat", 0))
    df_tx['lon'] = df_tx['Region'].map(lambda x: GEO_COORDS.get(x, {}).get("lon", 0))
    
    # Generate search links
    df_tx['Suggested_Search'] = df_tx.apply(lambda row: f"https://www.google.com/search?q={urllib.parse.quote_plus(f'\"{row.get(\"Buyer\",\"\")}\" \"{row.get(\"Seller\",\"\")}\" data center {row.get(\"Region\",\"\")}')}", axis=1)
    
    return df_tx
# --- EXECUTION ---
df = load_tx_data()
macro_df = load_live_macro_data()

# ==========================================
# 4. SIDEBAR & DASHBOARD
# ==========================================
st.sidebar.image("https://img.icons8.com/color/96/000000/server.png", width=60)
st.sidebar.header("🔍 Filter Dashboard")

all_regions = df['Region'].dropna().unique().tolist()
if not all_regions: all_regions = ["South Korea", "Hong Kong", "Japan", "Australia", "Singapore", "Malaysia"]

selected_regions = st.sidebar.multiselect("Select Regions to Benchmark:", all_regions, default=all_regions)
filtered_df = df[df['Region'].isin(selected_regions)]
filtered_macro_df = macro_df[macro_df['Region'].isin(selected_regions)]

st.title("🏢 APAC Data Centre Real Estate Tracker")

valid_valuation_deals = filtered_df[(filtered_df['Consideration_USD_M'] > 0) & (filtered_df['Capacity_MW'] > 0)]
total_val_usd = valid_valuation_deals['Consideration_USD_M'].sum()
total_val_mw = valid_valuation_deals['Capacity_MW'].sum()
avg_price_per_mw = (total_val_usd / total_val_mw) if total_val_mw > 0 else 0

total_usd = filtered_df['Consideration_USD_M'].sum()
total_mw = filtered_df['Capacity_MW'].sum()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Investment (Live FX)", f"USD {total_usd:,.1f} M")
m2.metric("Total Capacity", f"{total_mw:,.0f} MW")
m3.metric("Number of Deals", f"{len(filtered_df)}")
m4.metric("Avg. Price per MW", f"USD {avg_price_per_mw:,.1f} M") 
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Analytics & Map", "📈 Macro (Live API)", "🗄️ Database & Export", "📰 Live News Feed"])

# --- TAB 1: CHARTS AND MAP ---
with tab1:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("**Geographic Capacity Map**")
        if not filtered_df.empty:
            fig_map = px.scatter_geo(
                filtered_df, lat='lat', lon='lon', color='Region',
                size='Capacity_MW', hover_name='Asset',
                hover_data={'lat':False, 'lon':False, 'Capacity_MW':True, 'Consideration_USD_M':True},
                projection="natural earth", scope="asia", title="Bubble Size = MW"
            )
            fig_map.update_geos(fitbounds="locations", showcountries=True)
            fig_map.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_map, use_container_width=True)
    
    with c2:
        st.markdown("**Investment Volume by Region**")
        if not filtered_df.empty:
            fig_bar = px.bar(
                filtered_df.groupby('Region')['Consideration_USD_M'].sum().reset_index(),
                x='Region', y='Consideration_USD_M', color='Region', template='plotly_white'
            )
            fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="USD (Millions)")
            st.plotly_chart(fig_bar, use_container_width=True)

# --- TAB 2: MACROECONOMICS (LIVE API) ---
with tab2:
    st.markdown("**Live Regional Macroeconomic Indicators**")
    st.caption("Auto-fetching latest data from the World Bank API...")
    mc1, mc2 = st.columns(2)
    with mc1:
        fig_gdp = px.bar(filtered_macro_df, x="Region", y="GDP (USD Billions)", color="Region", title="GDP (USD Billions)", template="plotly_white")
        fig_gdp.update_layout(showlegend=False)
        st.plotly_chart(fig_gdp, use_container_width=True)
    with mc2:
        fig_pop = px.pie(filtered_macro_df, values="Population (Millions)", names="Region", title="Population Distribution", hole=0.4, template="plotly_white")
        st.plotly_chart(fig_pop, use_container_width=True)
        
    st.dataframe(filtered_macro_df, hide_index=True, use_container_width=True)

# --- TAB 3: DATABASE & EXCEL EXPORT ---
with tab3:
    st.markdown("**Transaction Database (Unified via Live FX)**")
    display_df = filtered_df[['Region', 'Asset', 'Buyer', 'Seller', 'GFA_sqm', 'Capacity_MW', 'Consideration_USD_M', 'Currency', 'Consideration_Value', 'Date', 'Suggested_Search', 'URL']].copy()
    display_df['Date'] = display_df['Date'].dt.strftime('%Y-%m-%d')
    
    st.dataframe(
        display_df,
        column_config={
            "GFA_sqm": st.column_config.NumberColumn("Unified GFA", format="%d sqm"),
            "Consideration_USD_M": st.column_config.NumberColumn("Unified Price", format="USD %d M"),
            "Consideration_Value": st.column_config.NumberColumn("Local Price"),
            "URL": st.column_config.LinkColumn("Provided Source", display_text="View Article"),
            "Suggested_Search": st.column_config.LinkColumn("Suggested Source", display_text="🔍 Search Web")
        }, hide_index=True, use_container_width=True
    )

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        display_df.drop(columns=['Suggested_Search']).to_excel(writer, index=False, sheet_name='APAC Transactions')
        filtered_macro_df.to_excel(writer, index=False, sheet_name='Live Macro Data')
    
    st.download_button("📥 Download Dashboard to Excel (.xlsx)", data=output.getvalue(), file_name="APAC_DC_Intelligence.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# --- TAB 4: FILTERABLE LIVE LOCAL NEWS FEED ---
with tab4:
    st.markdown("**Localized Market Intelligence Feed**")
    nc1, nc2 = st.columns(2)
    news_region = nc1.selectbox("Filter News by Region:", ["APAC (All)"] + all_regions)
    news_topic = nc2.selectbox("Filter by Topic:", ["Data Centers", "M&A", "Macroeconomics"])
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
        "South Korea": {"Data Centers": "", "M&A": "인수합병 OR M&A", "Macroeconomics": "금리 OR 거시경제"},
        "Japan": {"Data Centers": "", "M&A": "買収 OR M&A", "Macroeconomics": "金利 OR マクロ経済"},
        "Hong Kong": {"Data Centers": "", "M&A": "併購 OR M&A", "Macroeconomics": "利率 OR 宏觀經濟"},
        "Default": {"Data Centers": "", "M&A": "M&A OR acquisition", "Macroeconomics": "interest rates OR economy"}
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
                for entry in feed.entries[:12]: 
                    st.markdown(f"📰 **[{entry.title}]({entry.link})**")
                    pub_date = entry.published if hasattr(entry, 'published') else "Recent"
                    st.caption(f"Published: {pub_date}")
                    st.write("") 
        except Exception:
            st.warning("Could not load news feed at this time.")
            # --- TAB 5: ADMIN DATA ENTRY ---
with st.expander("🔐 Admin: Add New Transaction"):
    st.info("Fill out the details below to add a new deal to the SQLite database.")
    
    with st.form("admin_entry_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            new_region = st.selectbox("Region", list(GEO_COORDS.keys()))
            new_asset = st.text_input("Asset Name", placeholder="e.g. Sydney North DC")
            new_buyer = st.text_input("Buyer")
            new_seller = st.text_input("Seller")
            
        with col2:
            new_gfa = st.number_input("GFA Value", min_value=0.0)
            new_unit = st.selectbox("GFA Unit", ["sqm", "sqft", "acres"])
            new_mw = st.number_input("Capacity (MW)", min_value=0.0)
            
        with col3:
            new_price = st.number_input("Consideration (Local)", min_value=0.0)
            new_curr = st.selectbox("Currency", ["USD", "KRW", "HKD", "AUD", "SGD", "JPY"])
            new_date = st.date_input("Transaction Date")
            new_url = st.text_input("Source URL")

        new_remarks = st.text_area("Remarks")
        
        submit_button = st.form_submit_button("Add Transaction to Database")
        
        if submit_button:
            if new_asset and new_buyer:
                try:
                    conn = sqlite3.connect(DB_SQLITE)
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO transactions (
                            Region, Asset, Buyer, Seller, GFA_Value, GFA_Unit, 
                            Capacity_MW, Consideration_Value, Currency, Date, 
                            Remarks, Source, URL
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        new_region, new_asset, new_buyer, new_seller, new_gfa, new_unit,
                        new_mw, new_price, new_curr, str(new_date), new_remarks, "Manual Entry", new_url
                    ))
                    conn.commit()
                    conn.close()
                    st.success(f"✅ Successfully added {new_asset} to the database!")
                    st.cache_data.clear() # Clears cache so the new data shows up immediately
                    st.rerun() 
                except Exception as e:
                    st.error(f"Error updating database: {e}")
            else:
                st.warning("Please fill in at least the Asset Name and Buyer.")

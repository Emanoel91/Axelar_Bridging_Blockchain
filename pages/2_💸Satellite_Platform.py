import streamlit as st
st.write("🚀 تست تغییر کد: این متن باید ظاهر بشه")
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ---
st.set_page_config(
    page_title="Axelar: Bridging Blockchain",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Debug Timestamp (برای تست رفرش) ---
st.write("🔄 آخرین بار رفرش:", pd.Timestamp.now())

# --- Title ---
st.title("💸 Satellite Platform")

# --- Attention ---
st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px;
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ---
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=user,
        account=account,
        private_key=private_key_bytes,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

conn = init_connection()

# --- Date Inputs ---
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"]) 

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2024-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))  

# --- Query Function ---
def load_overview_data(start_date, end_date):
    query = f"""
    SELECT 
      COUNT(DISTINCT tx_hash) AS number_of_transfers, 
      COUNT(DISTINCT sender) AS number_of_users,
      ROUND(SUM(amount_usd)) AS volume_of_transfers_usd,
      ROUND(COUNT(DISTINCT tx_hash)/COUNT(DISTINCT sender)) AS avg_txn_count_per_user,
      ROUND(AVG(amount_usd)) AS avg_volume_per_txn,
      ROUND(SUM(amount_usd)/COUNT(DISTINCT sender)) AS avg_volume_per_user
    FROM some_table_in_snowflake  -- فقط برای تست اسم جدول رو درست بذار
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
try:
    df_kpi = load_overview_data(start_date, end_date)

    # --- KPI Cards ---
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Number of Transfers", f"{df_kpi['NUMBER_OF_TRANSFERS'].iloc[0]/1000:.1f}k Txns")

    with col2:
        st.metric("Number of Users", f"{df_kpi['NUMBER_OF_USERS'].iloc[0]/1000:.1f}k Wallets")

    with col3:
        st.metric("Volume of Transfers (USD)", f"{df_kpi['VOLUME_OF_TRANSFERS_USD'].iloc[0]/1_000_000:.1f}m $")

    with col4:
        st.metric("Avg Txn Count per User", f"{df_kpi['AVG_TXN_COUNT_PER_USER'].iloc[0]/1000:.1f}k Txns")

    with col5:
        st.metric("Avg Volume per Txn", f"{df_kpi['AVG_VOLUME_PER_TXN'].iloc[0]/1000:.1f}k $")

    with col6:
        st.metric("Avg Volume per User", f"{df_kpi['AVG_VOLUME_PER_USER'].iloc[0]/1000:.1f}k $")

except Exception as e:
    st.error(f"❌ خطا در بارگذاری داده: {e}")

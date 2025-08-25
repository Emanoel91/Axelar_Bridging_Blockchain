import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar: Bridging Blockchain",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title  -----------------------------------------------------------------------------------------------------
st.title("💸Satellite Platform")

# --- attention ---------------------------------------------------------------------------------------------------------
st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
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

# --- Snowflake Connection ----------------------------------------------------------------------------------------
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

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"]) 

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2024-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))  

# --- Cached Query ------------------------------------------------------------------------------------------------
@st.cache_data
def load_overview_data(start_date, end_date):
    query = f"""
    with overview as (
      with tab1 as (
        select block_timestamp::date as date, tx_hash, source_chain, destination_chain, sender, token_symbol
        from AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        
      ),
      tab2 as (
        SELECT 
            created_at::date as date, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            sender_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd, 
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee, 
            SPLIT_PART(id, '_', 1) as tx_hash, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' 
          AND simplified_status = 'received' 
          
      )
      select 
        tab1.date as date, 
        tab1.tx_hash as tx_hash, 
        tab1.source_chain as source_chain, 
        tab1.destination_chain as destination_chain, 
        sender, 
        token_symbol, 
        amount, 
        amount_usd
      from tab1 
      left join tab2 on tab1.tx_hash=tab2.tx_hash
    )
    select 
      count(distinct tx_hash) as number_of_transfers, 
      count(distinct sender) as number_of_users,
      round(sum(amount_usd)) as volume_of_transfers_usd,
      round(count(distinct tx_hash)/count(distinct sender)) as avg_txn_count_per_user,
      round(avg(amount_usd)) as avg_volume_per_txn,
      round(sum(amount_usd)/count(distinct sender)) as avg_volume_per_user
    from overview
    where date between '{start_date}' and '{end_date}'
    """
    return pd.read_sql(query, conn)

# --- Load Data ---------------------------------------------------------------------------------------------------
df_kpi = load_overview_data(start_date, end_date)

# --- KPI Cards ---------------------------------------------------------------------------------------------------
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric(
        label="Number of Transfers",
        value=f"{df_kpi['NUMBER_OF_TRANSFERS'].iloc[0]/1000:.1f}k Txns"
    )

with col2:
    st.metric(
        label="Number of Users",
        value=f"{df_kpi['NUMBER_OF_USERS'].iloc[0]/1000:.1f}k Wallets"
    )

with col3:
    st.metric(
        label="Volume of Transfers (USD)",
        value=f"{df_kpi['VOLUME_OF_TRANSFERS_USD'].iloc[0]/1_000_000:.1f}m $"
    )

with col4:
    st.metric(
        label="Avg Txn Count per User",
        value=f"{df_kpi['AVG_TXN_COUNT_PER_USER'].iloc[0]/1000:.1f}k Txns"
    )

with col5:
    st.metric(
        label="Avg Volume per Txn",
        value=f"{df_kpi['AVG_VOLUME_PER_TXN'].iloc[0]/1000:.1f}k $"
    )

with col6:
    st.metric(
        label="Avg Volume per User",
        value=f"{df_kpi['AVG_VOLUME_PER_USER'].iloc[0]/1000:.1f}k $"
    )

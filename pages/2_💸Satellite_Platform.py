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
st.info("📊Tables initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")
st.info("⭕The standalone Satellite interface is no longer available. The old satellite.money interface, which allowed users to transfer tokens across chains, has been deprecated. Now, satellite.money redirects you to the Squid Router interface.")

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
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

# --- Cached Query Execution ---------------------------------------------------------------------------------
# --- Row 1, 2 --------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_kpi_data(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            sender_address AS user,
            CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
            CASE 
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
              THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
            SPLIT_PART(id, '_', 1) as tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' 
          AND simplified_status = 'received'
          AND created_at::date >= '{start_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 
      LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      COUNT(DISTINCT tx_hash) AS transfers, 
      COUNT(DISTINCT sender) AS users,
      ROUND(SUM(amount_usd)) AS volume_usd,
      ROUND(COUNT(DISTINCT tx_hash)/COUNT(DISTINCT sender)) AS avg_tx_per_user,
      ROUND(AVG(amount_usd)) AS avg_volume_tx,
      ROUND(SUM(amount_usd)/COUNT(DISTINCT sender)) AS avg_volume_user
    FROM overview
    WHERE date >= '{start_date}' AND date <= '{end_date}';
    """
    df = pd.read_sql(query, _conn)
    return df.iloc[0]

# --- Load KPI Data from Snowflake ---------------------------
kpi_df = get_kpi_data(conn, start_date, end_date)

# --- Display KPI (Row 1 & 2) --------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**Number of Transfers**")
    st.markdown(f"{kpi_df['TRANSFERS']/1000:.1f}K Txns")
with col2:
    st.markdown("**Number of Users**")
    st.markdown(f"{kpi_df['USERS']/1000:.1f}K Wallets")
with col3:
    st.markdown("**Volume of Transfers**")
    st.markdown(f"${kpi_df['VOLUME_USD']/1_000_000:.1f}M")

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown("**Avg Txn count per User**")
    st.markdown(f"{kpi_df['AVG_TX_PER_USER']/1000:.1f}K Txns")
with col5:
    st.markdown("**Avg Volume per Txn**")
    st.markdown(f"${kpi_df['AVG_VOLUME_TX']/1000:.1f}K")
with col6:
    st.markdown("**Avg Volume per User**")
    st.markdown(f"${kpi_df['AVG_VOLUME_USER']/1000:.1f}K")

# --- Row 3 --------------------------------------------------------------------------------------------------------------------------------------------------------------------

@st.cache_data
def get_ts_data(_conn, start_date, end_date, timeframe):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            sender_address AS user,
            CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
            CASE 
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
              THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
            SPLIT_PART(id, '_', 1) as tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' 
          AND simplified_status = 'received'
          AND created_at::date >= '{start_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 
      LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      DATE_TRUNC('{timeframe}', date) AS date,
      COUNT(DISTINCT tx_hash) AS transfers, 
      COUNT(DISTINCT sender) AS users,
      ROUND(SUM(amount_usd)) AS volume_usd,
      ROUND(AVG(amount_usd)) AS avg_volume_tx
    FROM overview
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    GROUP BY 1
    ORDER BY 1;
    """
    df = pd.read_sql(query, _conn)
    return df
# --- Load Time-Series Data from Snowflake -------------------
ts_df = get_ts_data(conn, start_date, end_date, timeframe)

# --- Display Charts (Row 3) ---------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_bar(x=ts_df["DATE"], y=ts_df["TRANSFERS"], name="Transfers", yaxis="y1")
    fig1.add_trace(go.Scatter(x=ts_df["DATE"], y=ts_df["USERS"], name="Users", mode="lines+markers", yaxis="y2"))
    fig1.update_layout(
        title="Number of Transfers & Users Over Time",
        yaxis=dict(title="Txns count"),
        yaxis2=dict(title="Wallet count", overlaying="y", side="right"),
        xaxis=dict(title=" "),
        barmode="group"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_bar(x=ts_df["DATE"], y=ts_df["VOLUME_USD"], name="Volume (USD)", yaxis="y1")
    fig2.add_trace(go.Scatter(x=ts_df["DATE"], y=ts_df["AVG_VOLUME_TX"], name="Avg Volume/Txn", mode="lines+markers", yaxis="y2"))
    fig2.update_layout(
        title="Volume of Transfers Over Time",
        yaxis=dict(title="$USD"),
        yaxis2=dict(title="$USD", overlaying="y", side="right"),
        xaxis=dict(title=" "),
        barmode="group"
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_source_chain_summary(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
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
            SPLIT_PART(id, '_', 1) AS tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' AND simplified_status = 'received'
          AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      source_chain AS "Source Chain",
      COUNT(DISTINCT tx_hash) AS "Number of Transfers",
      COUNT(DISTINCT sender) AS "Number of Users",
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM overview
    WHERE source_chain IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Source Chain Summary Data ---------------------------------------------------------------------------
df_source_chain = get_source_chain_summary(conn, start_date, end_date)

# --- Display Charts ------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Clustered Bar Chart: Transfers & Users
with col1:
    fig_bar = go.Figure()
    fig_bar.add_bar(x=df_source_chain["Source Chain"], y=df_source_chain["Number of Transfers"], name="Number of Transfers", yaxis="y1")
    fig_bar.add_trace(go.Scatter(x=df_source_chain["Source Chain"], y=df_source_chain["Number of Users"], name="Number of Users", mode="lines+markers", yaxis="y2"))
    fig_bar.update_layout(
        title="Total Number of Transfers & Users by Source Chain",
        yaxis=dict(title="Txns count"),
        yaxis2=dict(title="Wallet count", overlaying="y", side="right"),
        xaxis=dict(title="Source Chain"),
        barmode="group"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# Donut Chart: Volume of Transfers by Source Chain
with col2:
    fig_donut = go.Figure(data=[go.Pie(
        labels=df_source_chain["Source Chain"], 
        values=df_source_chain["Volume of Transfers (USD)"], 
        hole=0.5
    )])
    fig_donut.update_layout(
        title="Total Volume of Transfers by Source Chain ($USD)"
    )
    st.plotly_chart(fig_donut, use_container_width=True)

# --- Row 5 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_destination_chain_summary(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
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
            SPLIT_PART(id, '_', 1) AS tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' AND simplified_status = 'received'
          AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      destination_chain AS "Destination Chain",
      COUNT(DISTINCT tx_hash) AS "Number of Transfers",
      COUNT(DISTINCT sender) AS "Number of Users",
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM overview
    WHERE destination_chain IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Destination Chain Summary Data ----------------------------------------------------------------------
df_destination_chain = get_destination_chain_summary(conn, start_date, end_date)

# --- Display Charts --------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Clustered Horizontal Bar Chart: Transfers & Users
with col1:
    fig_hbar = go.Figure()
    fig_hbar.add_bar(y=df_destination_chain["Destination Chain"], x=df_destination_chain["Number of Transfers"], name="Number of Transfers", orientation='h')
    fig_hbar.add_bar(y=df_destination_chain["Destination Chain"], x=df_destination_chain["Number of Users"], name="Number of Users", orientation='h')
    fig_hbar.update_layout(
        title="Total Number of Transfers & Users by Destination Chain",
        barmode='group',
        xaxis=dict(title=" "),
        yaxis=dict(title="Destination Chain")
    )
    st.plotly_chart(fig_hbar, use_container_width=True)

# Pie Chart: Volume of Transfers by Destination Chain
with col2:
    fig_pie = go.Figure(data=[go.Pie(
        labels=df_destination_chain["Destination Chain"],
        values=df_destination_chain["Volume of Transfers (USD)"]
    )])
    fig_pie.update_layout(
        title="Total Volume of Transfers by Destination Chain (USD)"
    )
    st.plotly_chart(fig_pie, use_container_width=True)


# --- Row 6 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_token_summary(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
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
            SPLIT_PART(id, '_', 1) AS tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' AND simplified_status = 'received'
          AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      token_symbol AS "Token",
      COUNT(DISTINCT tx_hash) AS "Number of Transfers",
      COUNT(DISTINCT sender) AS "Number of Users",
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM overview
    WHERE destination_chain IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Token Summary Data ----------------------------------------------------------------------
df_token = get_token_summary(conn, start_date, end_date)

# --- Display Charts --------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Clustered Horizontal Bar Chart: Transfers & Users
with col1:
    fig_hbar = go.Figure()
    fig_hbar.add_bar(y=df_token["Token"], x=df_token["Number of Transfers"], name="Number of Transfers", orientation='h')
    fig_hbar.add_bar(y=df_token["Token"], x=df_token["Number of Users"], name="Number of Users", orientation='h')
    fig_hbar.update_layout(
        title="Total Number of Transfers & Users by Token",
        barmode='group',
        xaxis=dict(title=" "),
        yaxis=dict(title="Token Symbol")
    )
    st.plotly_chart(fig_hbar, use_container_width=True)

# Pie Chart: Volume of Transfers by Token
with col2:
    fig_pie = go.Figure(data=[go.Pie(
        labels=df_token["Token"],
        values=df_token["Volume of Transfers (USD)"],
        textinfo='label+percent',       
        textposition='inside',          
        insidetextorientation='radial'  
    )])
    fig_pie.update_layout(
        title="Total Volume of Transfers by Token (USD)"
    )
    st.plotly_chart(fig_pie, use_container_width=True)

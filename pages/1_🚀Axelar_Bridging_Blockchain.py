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
    page_title="Axelar: Bridging Blockchain Ecosystems",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title  -----------------------------------------------------------------------------------------------------
st.title("🚀Axelar Bridging Blockchain")

# --- attention ---------------------------------------------------------------------------------------------------------
st.info("📊Tables initially display data for a default time range. Select a custom range to view results for your desired period.")
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
col1, col2 = st.columns(2) 

with col1:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2025-01-01"))

with col2: 
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

# --- Cached Query Execution ---------------------------------------------------------------------------------
@st.cache_data
def get_source_chain_data(_conn, start_date, end_date):
    query = f"""
    with overview as (
WITH axelar_service AS (
  
  SELECT 
    created_at, 
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

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee, raw_asset

FROM axelar_service)

select source_chain as "📤Source Chain", count(distinct id) as "🚀Transfers",
count(distinct user) as "👥Users", round(sum(amount_usd),1) as "💸Volume($)",
round(avg(amount_usd),1) as "📊Avg Volume($)", round(sum(fee),1) as "⛽Fees($)",
round(avg(fee),5) as "💨Avg Fee($)", count(distinct destination_chain) as "📥#Dest Chains",
count(distinct raw_asset) as "💎#Tokens"
from overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}' and source_chain is not null
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_source_chains = get_source_chain_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_source_chains.copy()
for col in df_display.columns[1:]:
    if col == "💨Avg Fee($)":
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.3f}" if pd.notnull(x) else "-")
    else:
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("1️⃣Monitoring Source Chains")
st.dataframe(df_display, height=400)

# --- KPIs --------------------------------------------------------------------------------------------------------

top_transfers = df_source_chains.loc[df_source_chains["🚀Transfers"].idxmax()]
top_users = df_source_chains.loc[df_source_chains["👥Users"].idxmax()]
top_volume = df_source_chains.loc[df_source_chains["💸Volume($)"].idxmax()]

top_fees = df_source_chains.loc[df_source_chains["⛽Fees($)"].idxmax()]
top_dest_chains = df_source_chains.loc[df_source_chains["📥#Dest Chains"].idxmax()]
top_by_destination_chain_count = df_source_chains.loc[df_source_chains["💎#Tokens"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        "Top Source Chain by Transfers Count",
        f"{top_transfers['📤Source Chain']} ({top_transfers['🚀Transfers'] / 1_000:.1f}k)"
    )
with col2:
    st.metric(
        "Top Source Chain by Users Count",
        f"{top_users['📤Source Chain']} ({top_users['👥Users'] / 1_000:.1f}k)"
    )
with col3:
    st.metric(
        "Top Source Chain by Transfers Volume (USD)",
        f"{top_volume['📤Source Chain']} (${top_volume['💸Volume($)'] / 1_000_000:.2f}m)"
    )

col4, col5, col6 = st.columns(3)
with col4:
    st.metric(
        "Top Source Chain by Transfer Fees (USD)",
        f"{top_fees['📤Source Chain']} (${top_fees['⛽Fees($)'] / 1_000:.1f}k)"
    )
with col5:
    st.metric(
        "Top Source Chain by Number of Destination Chains",
        f"{top_dest_chains['📤Source Chain']} ({top_dest_chains['📥#Dest Chains']:,})"
    )
with col6:
    st.metric(
        "Top Source Chain by Number of Tokens Transferred",
        f"{top_by_destination_chain_count['📤Source Chain']} ({top_by_destination_chain_count['💎#Tokens']:,})"
    )

# --- Destination Chain Stats -----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_destination_chain_data(_conn, start_date, end_date):
    query = f"""
    with overview as (
WITH axelar_service AS (
  
  SELECT 
    created_at, 
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

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee, raw_asset

FROM axelar_service)

select destination_chain as "📥Destination Chain", count(distinct id) as "🚀Transfers",
count(distinct user) as "👥Users", round(sum(amount_usd),1) as "💸Volume($)",
round(avg(amount_usd),1) as "📊Avg Volume($)", round(sum(fee),1) as "⛽Fees($)",
round(avg(fee),5) as "💨Avg Fee($)", count(distinct source_chain) as "📤#Source Chains",
count(distinct raw_asset) as "💎#Tokens"
from overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}' and destination_chain is not null
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_destination_chains = get_destination_chain_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_destination_chains.copy()
for col in df_display.columns[1:]:
    if col == "💨Avg Fee($)":
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.3f}" if pd.notnull(x) else "-")
    else:
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("2️⃣Monitoring Destination Chains")
st.dataframe(df_display, height=400)

# --- KPIs --------------------------------------------------------------------------------------------------------

top_transfers = df_destination_chains.loc[df_destination_chains["🚀Transfers"].idxmax()]
top_users = df_destination_chains.loc[df_destination_chains["👥Users"].idxmax()]
top_volume = df_destination_chains.loc[df_destination_chains["💸Volume($)"].idxmax()]

top_fees = df_destination_chains.loc[df_destination_chains["⛽Fees($)"].idxmax()]
top_by_source_chain_count = df_destination_chains.loc[df_destination_chains["📤#Source Chains"].idxmax()]
top_by_destination_chain_count = df_destination_chains.loc[df_destination_chains["💎#Tokens"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        "Top Destination Chain by Transfers Count",
        f"{top_transfers['📥Destination Chain']} ({top_transfers['🚀Transfers'] / 1_000:.1f}k)"
    )
with col2:
    st.metric(
        "Top Destination Chain by Users Count",
        f"{top_users['📥Destination Chain']} ({top_users['👥Users'] / 1_000:.1f}k)"
    )
with col3:
    st.metric(
        "Top Destination Chain by Transfers Volume (USD)",
        f"{top_volume['📥Destination Chain']} (${top_volume['💸Volume($)'] / 1_000_000:.2f}m)"
    )

col4, col5, col6 = st.columns(3)
with col4:
    st.metric(
        "Top Destination Chain by Transfer Fees (USD)",
        f"{top_fees['📥Destination Chain']} (${top_fees['⛽Fees($)'] / 1_000:.1f}k)"
    )
with col5:
    st.metric(
        "Top Destination Chain by Number of Source Chains",
        f"{top_by_source_chain_count['📥Destination Chain']} ({top_by_source_chain_count['📤#Source Chains']:,})"
    )
with col6:
    st.metric(
        "Top Destination Chain by Number of Tokens Transferred",
        f"{top_by_destination_chain_count['📥Destination Chain']} ({top_by_destination_chain_count['💎#Tokens']:,})"
    )

# ---Cross-chain Path Analysis --------------------------------------------------------------------------------------------------------------------------------------------------------

@st.cache_data
def get_path_chain_data(_conn, start_date, end_date):
    query = f"""
    with overview as (
WITH axelar_service AS (
  
  SELECT 
    created_at, 
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

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee, raw_asset

FROM axelar_service)

select source_chain || '➡' || destination_chain as "🔀Path", count(distinct id) as "🚀Transfers",
count(distinct user) as "👥Users", round(sum(amount_usd),1) as "💸Volume($)",
round(avg(amount_usd),1) as "📊Avg Volume($)", round(sum(fee),1) as "⛽Fees($)",
round(avg(fee),5) as "💨Avg Fee($)", round(count(distinct id)/count(distinct user)) as "📋Txn/User",
count(distinct raw_asset) as "💎#Tokens"
from overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}' and destination_chain is not null
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_path_chains = get_path_chain_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_path_chains.copy()
for col in df_display.columns[1:]:
    if col == "💨Avg Fee($)":
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.3f}" if pd.notnull(x) else "-")
    else:
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("3️⃣Monitoring Cross-Chain Paths")
st.dataframe(df_display, height=400)

# --- KPIs --------------------------------------------------------------------------------------------------------

top_transfers = df_path_chains.loc[df_path_chains["🚀Transfers"].idxmax()]
top_users = df_path_chains.loc[df_path_chains["👥Users"].idxmax()]
top_volume = df_path_chains.loc[df_path_chains["💸Volume($)"].idxmax()]

top_fees = df_path_chains.loc[df_path_chains["⛽Fees($)"].idxmax()]
top_by_source_chain_count = df_path_chains.loc[df_path_chains["📋Txn/User"].idxmax()]
top_by_destination_chain_count = df_path_chains.loc[df_path_chains["💎#Tokens"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(
        f"""
        **Top Path by Transfers Count**  

        {top_transfers['🔀Path']}  
        **{top_transfers['🚀Transfers'] / 1_000:.1f}k**
        """
    )
with col2:
    st.markdown(
        f"""
        **Top Path by Users Count**  

        {top_users['🔀Path']}  
        **{top_users['👥Users'] / 1_000:.1f}k**
        """
    )
with col3:
    st.markdown(
        f"""
        **Top Path by Transfers Volume (USD)**  

        {top_volume['🔀Path']}  
        **${top_volume['💸Volume($)'] / 1_000_000:.2f}m**
        """
    )

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(
        f"""
        **Top Path by Transfer Fees (USD)**  

        {top_fees['🔀Path']}  
        **${top_fees['⛽Fees($)'] / 1_000:.1f}k**
        """
    )
with col5:
    st.markdown(
        f"""
        **Top Path by Avg Txn per User**  

        {top_by_source_chain_count['🔀Path']}  
        **{top_by_source_chain_count['📋Txn/User']:,}**
        """
    )
with col6:
    st.markdown(
        f"""
        **Top Path by Number of Tokens Transferred**  

        {top_by_destination_chain_count['🔀Path']}  
        **{top_by_destination_chain_count['💎#Tokens']:,}**
        """
    )

# --- Asset Stats -----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_token_data(_conn, start_date, end_date):
    query = f"""
    with overview as (
WITH axelar_service AS (
  
  SELECT 
    created_at, 
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

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    )

SELECT created_at, id, user, source_chain, destination_chain,
     "Service", amount, amount_usd, fee, CASE 
      WHEN raw_asset='arb-wei' THEN 'ARB'
      WHEN raw_asset='avalanche-uusdc' THEN 'Avalanche USDC'
      WHEN raw_asset='avax-wei' THEN 'AVAX'
      WHEN raw_asset='bnb-wei' THEN 'BNB'
      WHEN raw_asset='busd-wei' THEN 'BUSD'
      WHEN raw_asset='cbeth-wei' THEN 'cbETH'
      WHEN raw_asset='cusd-wei' THEN 'cUSD'
      WHEN raw_asset='dai-wei' THEN 'DAI'
      WHEN raw_asset='dot-planck' THEN 'DOT'
      WHEN raw_asset='eeur' THEN 'EURC'
      WHEN raw_asset='ern-wei' THEN 'ERN'
      WHEN raw_asset='eth-wei' THEN 'ETH'
      WHEN raw_asset ILIKE 'factory/sei10hub%' THEN 'SEILOR'
      WHEN raw_asset='fil-wei' THEN 'FIL'
      WHEN raw_asset='frax-wei' THEN 'FRAX'
      WHEN raw_asset='ftm-wei' THEN 'FTM'
      WHEN raw_asset='glmr-wei' THEN 'GLMR'
      WHEN raw_asset='hzn-wei' THEN 'HZN'
      WHEN raw_asset='link-wei' THEN 'LINK'
      WHEN raw_asset='matic-wei' THEN 'MATIC'
      WHEN raw_asset='mkr-wei' THEN 'MKR'
      WHEN raw_asset='mpx-wei' THEN 'MPX'
      WHEN raw_asset='oath-wei' THEN 'OATH'
      WHEN raw_asset='op-wei' THEN 'OP'
      WHEN raw_asset='orbs-wei' THEN 'ORBS'
      WHEN raw_asset='factory/sei10hud5e5er4aul2l7sp2u9qp2lag5u4xf8mvyx38cnjvqhlgsrcls5qn5ke/seilor' THEN 'SEILOR'
      WHEN raw_asset='pepe-wei' THEN 'PEPE'
      WHEN raw_asset='polygon-uusdc' THEN 'Polygon USDC'
      WHEN raw_asset='reth-wei' THEN 'rETH'
      WHEN raw_asset='ring-wei' THEN 'RING'
      WHEN raw_asset='shib-wei' THEN 'SHIB'
      WHEN raw_asset='sonne-wei' THEN 'SONNE'
      WHEN raw_asset='stuatom' THEN 'stATOM'
      WHEN raw_asset='uatom' THEN 'ATOM'
      WHEN raw_asset='uaxl' THEN 'AXL'
      WHEN raw_asset='ukuji' THEN 'KUJI'
      WHEN raw_asset='ulava' THEN 'LAVA'
      WHEN raw_asset='uluna' THEN 'LUNA'
      WHEN raw_asset='ungm' THEN 'NGM'
      WHEN raw_asset='uni-wei' THEN 'UNI'
      WHEN raw_asset='uosmo' THEN 'OSMO'
      WHEN raw_asset='usomm' THEN 'SOMM'
      WHEN raw_asset='ustrd' THEN 'STRD'
      WHEN raw_asset='utia' THEN 'TIA'
      WHEN raw_asset='uumee' THEN 'UMEE'
      WHEN raw_asset='uusd' THEN 'USTC'
      WHEN raw_asset='uusdc' THEN 'USDC'
      WHEN raw_asset='uusdt' THEN 'USDT'
      WHEN raw_asset='vela-wei' THEN 'VELA'
      WHEN raw_asset='wavax-wei' THEN 'WAVAX'
      WHEN raw_asset='wbnb-wei' THEN 'WBNB'
      WHEN raw_asset='wbtc-satoshi' THEN 'WBTC'
      WHEN raw_asset='weth-wei' THEN 'WETH'
      WHEN raw_asset='wfil-wei' THEN 'WFIL'
      WHEN raw_asset='wftm-wei' THEN 'WFTM'
      WHEN raw_asset='wglmr-wei' THEN 'WGLMR'
      WHEN raw_asset='wmai-wei' THEN 'WMAI'
      WHEN raw_asset='wmatic-wei' THEN 'WMATIC'
      WHEN raw_asset='wsteth-wei' THEN 'wstETH'
      WHEN raw_asset='yield-eth-wei' THEN 'yieldETH' 
      else raw_asset end as "Symbol"

FROM axelar_service)

select "Symbol" as "💎Token", count(distinct id) as "🚀Transfers",
count(distinct user) as "👥Users", round(sum(amount_usd),1) as "💸Volume($)",
round(avg(amount_usd),1) as "📊Avg Volume($)", round(sum(fee),1) as "⛽Fees($)",
round(avg(fee),5) as "💨Avg Fee($)", count(distinct source_chain) as "📤#Source Chains",
count(distinct destination_chain) as "📥#Destination Chains"
from overview
    WHERE created_at::date >= '{start_date}' AND created_at::date <= '{end_date}' and "Symbol" is not null
    GROUP BY 1
    ORDER BY 2 DESC
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data from Snowflake ---------------------------------------------------------------------------------
df_token = get_token_data(conn, start_date, end_date)

# --- Format Numbers and Reset Index Starting from 1 ------------------------------------------------------------
df_display = df_token.copy()
for col in df_display.columns[1:]:
    if col == "💨Avg Fee($)":
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.3f}" if pd.notnull(x) else "-")
    else:
        df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "-")
df_display.index = range(1, len(df_display)+1)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("4️⃣Monitoring Tokens")
st.dataframe(df_display, height=400)

# --- KPIs --------------------------------------------------------------------------------------------------------

top_transfers = df_token.loc[df_token["🚀Transfers"].idxmax()]
top_users = df_token.loc[df_token["👥Users"].idxmax()]
top_volume = df_token.loc[df_token["💸Volume($)"].idxmax()]

top_fees = df_token.loc[df_token["⛽Fees($)"].idxmax()]
top_by_source_chain_count = df_token.loc[df_token["📤#Source Chains"].idxmax()]
top_by_destination_chain_count = df_token.loc[df_token["📥#Destination Chains"].idxmax()]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        "Top Token by Transfers Count",
        f"{top_transfers['💎Token']} ({top_transfers['🚀Transfers'] / 1_000:.1f}k)"
    )
with col2:
    st.metric(
        "Top Token by Users Count",
        f"{top_users['💎Token']} ({top_users['👥Users'] / 1_000:.1f}k)"
    )
with col3:
    st.metric(
        "Top Token by Transfers Volume (USD)",
        f"{top_volume['💎Token']} (${top_volume['💸Volume($)'] / 1_000_000:.2f}m)"
    )

col4, col5, col6 = st.columns(3)
with col4:
    st.metric(
        "Top Token by Transfer Fees (USD)",
        f"{top_fees['💎Token']} (${top_fees['⛽Fees($)'] / 1_000:.1f}k)"
    )
with col5:
    st.metric(
        "Top Token by Number of Source Chains",
        f"{top_by_source_chain_count['💎Token']} ({top_by_source_chain_count['📤#Source Chains']:,})"
    )
with col6:
    st.metric(
        "Top Token by Number of Destination Chains",
        f"{top_by_destination_chain_count['💎Token']} ({top_by_destination_chain_count['📥#Destination Chains']:,})"
    )

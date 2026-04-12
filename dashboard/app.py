import streamlit as st
from queries import get_ohlcv_summary, get_moving_averages
import yaml
import os
import plotly.graph_objects as go
st.set_page_config(
    page_title="Stock Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(BASE_DIR, "config", "stocks.yaml"), "r") as f:
    config = yaml.safe_load(f)

stock_list = config["stocks"]
stock_list = list(map(lambda x: x.split(".")[0], stock_list))

@st.cache_data(show_spinner=False)
def load_ohlcv_summary(symbol):
    return get_ohlcv_summary(symbol)

@st.cache_data(show_spinner=False)
def load_moving_averages(symbol, period):
    return get_moving_averages(symbol, period)


symbol = st.selectbox("Select Stock", stock_list)

def show_stock_summary(data):
    row1 = st.columns(3)
    row2 = st.columns(3)

    with row1[0]:
        st.metric("Today Open", f"₹{data['Latest_Open']:.2f}")

    with row1[1]:
        st.metric("Prev Close", f"₹{data['Prev_Close']:.2f}")

    with row1[2]:
        st.metric("Today High", f"₹{data['Latest_High']:.2f}")

    with row2[0]:
        st.metric("Today Low", f"₹{data['Latest_Low']:.2f}")

    with row2[1]:
        st.metric("52W High", f"₹{data['High_52W']:.2f}")

    with row2[2]:
        st.metric("52W Low", f"₹{data['Low_52W']:.2f}")

with st.spinner("Loading stock data..."):
    ohlcv_summary = load_ohlcv_summary(symbol)
last_close = ohlcv_summary["Latest_Close"].iloc[0]
return_1d = ohlcv_summary["return_1d"].iloc[0]
prev_close = last_close / (1 + return_1d)
data = ohlcv_summary.iloc[0].to_dict()
data["Prev_Close"] = prev_close
st.metric(label="Stock Price", value=f"₹{last_close:.2f}", delta=f"{return_1d*100:.2f}%")
show_stock_summary(data)

st.markdown("### Price Chart")
period = st.segmented_control(
    "Range",
    ["1W", "1M", "3M", "6M", "1Y", "5Y", "MAX"],
    default = "1M"
)

moving_average = load_moving_averages(symbol, period.lower() if period else "1m")
moving_average = moving_average.sort_values("Date")
df = moving_average.set_index("Date")

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df.index,
    y=df["Close"],
    mode='lines',
    name='Close',
    line=dict(width=2)
))

fig.add_trace(go.Scatter(
    x=df.index,
    y=df["Moving_Average_7"],
    mode='lines',
    name='MA 7',
    line=dict(width=1, dash='dot')
))

fig.add_trace(go.Scatter(
    x=df.index,
    y=df["Moving_Average_21"],
    mode='lines',
    name='MA 21',
    line=dict(width=1, dash='dash')
))

fig.update_layout(
    title="",
    xaxis_title="Date",
    yaxis_title="Price",
    template="plotly_dark",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

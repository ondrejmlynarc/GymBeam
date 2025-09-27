import streamlit as st
import pandas as pd
import plotly.express as px
import os

# -----------------------------
# 1. Page configuration
# -----------------------------
st.set_page_config(
    page_title="GymBeam Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------
# 2. Custom CSS styling
# -----------------------------
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        border-bottom: 3px solid #1f77b4;
        padding-bottom: 1rem;
    }
    .section-header {
        font-size: 1.8rem;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-left: 10px;
        border-left: 4px solid #3498db;
    }
    .metric-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #28a745;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# 3. Load data
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # /workspaces/GymBeam/task_2
DATA_DIR = os.path.join(BASE_DIR, "data", "out")

@st.cache_data
def load_data():
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders_enriched.csv"), parse_dates=['created_at'])
    items = pd.read_csv(os.path.join(DATA_DIR, "items_cleaned.csv"))
    top5 = pd.read_csv(os.path.join(DATA_DIR, "top_5_city_recommendations.csv"))
    monthly_margin_df = pd.read_csv(os.path.join(DATA_DIR, "monthly_product_margin.csv"), parse_dates=['year_month'])
    top_pairs_df = pd.read_csv(os.path.join(DATA_DIR, "top_10_product_pairs.csv"))
    return orders, items, top5, monthly_margin_df, top_pairs_df
orders, items, top5, monthly_margin_df, top_pairs_df = load_data()


# For local testing purposes
# @st.cache_data
# def load_data():
#     orders = pd.read_csv("../data/out/orders_enriched.csv", parse_dates=['created_at'])
#     items = pd.read_csv("../data/out/items_cleaned.csv")
#     top5 = pd.read_csv("../data/out/top_5_city_recommendations.csv")
#     monthly_margin_df = pd.read_csv("../data/out/monthly_product_margin.csv", parse_dates=['year_month'])
#     top_pairs_df = pd.read_csv("../data/out/top_10_product_pairs.csv")
#     return orders, items, top5, monthly_margin_df, top_pairs_df

# orders, items, top5, monthly_margin_df, top_pairs_df = load_data()


# -----------------------------
# 4. Main title
# -----------------------------
st.markdown('<h1 class="main-header">GymBeam Analytics Dashboard</h1>', unsafe_allow_html=True)

# -----------------------------
# 5. Sidebar filters
# -----------------------------
st.sidebar.header("Filters & Controls")

if 'created_at' in orders.columns:
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(orders['created_at'].min().date(), orders['created_at'].max().date()),
        min_value=orders['created_at'].min().date(),
        max_value=orders['created_at'].max().date()
    )
    orders_filtered = orders[
        (orders['created_at'].dt.date >= date_range[0]) & 
        (orders['created_at'].dt.date <= date_range[1])
    ]
else:
    orders_filtered = orders

# -----------------------------
# 6. Key metrics
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_orders = len(orders_filtered)
    st.metric("Total Orders", f"{total_orders:,}")

with col2:
    total_revenue = orders_filtered['order_value'].sum()
    if total_revenue >= 1000000:
        revenue_formatted = f"€{total_revenue/1000000:.1f}M"
    elif total_revenue >= 1000:
        revenue_formatted = f"€{total_revenue/1000:.0f}K"
    else:
        revenue_formatted = f"€{total_revenue:.2f}"
    st.metric("Total Revenue", revenue_formatted)

with col3:
    avg_order_value = orders_filtered['order_value'].mean()
    st.metric("Average Order Value", f"€{avg_order_value:.2f}")

with col4:
    unique_cities = orders_filtered['place_name'].nunique()
    st.metric("Cities Served", unique_cities)

# -----------------------------
# 7. City performance analysis
# -----------------------------
st.markdown('<div class="section-header">City Performance Analysis</div>', unsafe_allow_html=True)

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Top 20 Cities by Average Order Value")
    aov_by_city = orders_filtered.groupby("place_name").agg({"order_value": "mean"}).reset_index().rename(columns={"order_value": "AOV"})
    top_20_cities = aov_by_city.sort_values("AOV", ascending=False).head(20)
    
    fig_bar = px.bar(
        top_20_cities,
        x="AOV",
        y="place_name",
        orientation='h',
        labels={"place_name": "City", "AOV": "Average Order Value (€)"},
        color="AOV",
        color_continuous_scale="Viridis",
        height=500
    )
    fig_bar.update_layout(
        coloraxis_showscale=False, 
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.subheader("Geographic Distribution of Orders")
    orders_count = orders_filtered.groupby(["place_name", "latitude", "longitude"]).size().reset_index(name="num_orders")
    
    fig_map = px.scatter_mapbox(
        orders_count.dropna(subset=['latitude', 'longitude']),
        lat="latitude",
        lon="longitude",
        size="num_orders",
        color="num_orders",
        hover_name="place_name",
        hover_data={"latitude": False, "longitude": False, "num_orders": True},
        zoom=5,
        mapbox_style="carto-positron",
        color_continuous_scale="Viridis",
        size_max=30,
        height=500
    )
    fig_map.update_layout(
        coloraxis_colorbar=dict(title="Orders", orientation="h", x=0.5, y=-0.15, xanchor="center"),
        margin=dict(b=80)
    )
    st.plotly_chart(fig_map, use_container_width=True)

# -----------------------------
# 8. Store expansion analysis
# -----------------------------
st.markdown('<div class="section-header">Store Expansion Recommendations</div>', unsafe_allow_html=True)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Top 5 Candidate Cities for New Stores")
    top_city = top5.sort_values("total_sales", ascending=False).iloc[0]
    top5['highlight'] = top5['place_name'] == top_city['place_name']

    fig_recommend = px.scatter_mapbox(
        top5,
        lat="latitude",
        lon="longitude",
        size="total_sales",
        color="highlight",
        color_discrete_map={True: "red", False: "blue"},
        hover_name="place_name",
        hover_data={"latitude": False, "longitude": False, "total_sales": True, "min_distance_km": True},
        zoom=5,
        mapbox_style="carto-positron",
        height=500
    )
    fig_recommend.update_layout(showlegend=False)
    st.plotly_chart(fig_recommend, use_container_width=True)

with col2:
    st.subheader("Recommendation Details")
    candidates_table = top5[['place_name', 'total_sales', 'min_distance_km']].sort_values('total_sales', ascending=False)
    candidates_table.columns = ['City', 'Total Sales (€)', 'Distance (km)']
    st.dataframe(candidates_table, use_container_width=True, hide_index=True)

# -----------------------------
# 9. Product analysis
# -----------------------------
st.markdown('<div class="section-header">Product Analysis</div>', unsafe_allow_html=True)

col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("Monthly Product Margin Analysis")
    product_names = sorted(monthly_margin_df['fk_item'].unique())
    selected_product = st.selectbox('Select Product:', product_names, key="product_selector")
    filtered_margin = monthly_margin_df[monthly_margin_df['fk_item'] == selected_product].copy()
    
    filtered_margin['year_month'] = pd.to_datetime(filtered_margin['year_month'])
    filtered_margin = filtered_margin.sort_values('year_month')
    
    fig_margin = px.line(
        filtered_margin, 
        x='year_month', 
        y='avg_margin', 
        title=f'Monthly Margin Trend - Product {selected_product}',
        labels={'year_month': 'Month', 'avg_margin': 'Average Margin'},
        markers=True
    )
    
    fig_margin.update_traces(line=dict(width=3))
    fig_margin.update_layout(
        xaxis=dict(title='Month', tickformat="%Y-%m"),
        yaxis=dict(title='Average Margin'),
        height=450
    )
    st.plotly_chart(fig_margin, use_container_width=True)

with col2:
    st.subheader("Top Product Combinations")
    top_pairs_df['Product Pair'] = top_pairs_df['product_1'].astype(str) + " + " + top_pairs_df['product_2'].astype(str)
    top_pairs_df['Percent'] = top_pairs_df['percent_of_orders'].round(2)
    
    pairs_display = top_pairs_df[['Product Pair', 'count', 'Percent']].head(10).copy()
    pairs_display.columns = ['Product Combination', 'Orders', 'Percentage (%)']
    pairs_display.index = range(1, len(pairs_display) + 1)
    st.dataframe(pairs_display, use_container_width=True, height=450)

# -----------------------------
# 10. Detailed data tables
# -----------------------------
with st.expander("View Detailed Data Tables"):
    tab1, tab2, tab3 = st.tabs(["City Rankings", "Product Pairs", "Margin Data"])
    
    with tab1:
        st.subheader("Complete City Rankings by AOV")
        full_aov_ranking = aov_by_city.sort_values("AOV", ascending=False)
        full_aov_ranking['AOV'] = full_aov_ranking['AOV'].round(2)
        st.dataframe(full_aov_ranking, use_container_width=True)
    
    with tab2:
        st.subheader("All Product Combinations")
        pairs_display = top_pairs_df[['Product Pair', 'count', 'Percent']].copy()
        pairs_display.columns = ['Product Combination', 'Orders', 'Percentage (%)']
        st.dataframe(pairs_display, use_container_width=True)
    
    with tab3:
        st.subheader("Product Margin History")
        margin_display = monthly_margin_df.copy()
        margin_display['avg_margin'] = margin_display['avg_margin'].round(2)
        st.dataframe(margin_display, use_container_width=True)

# -----------------------------
# 11. Footer
# -----------------------------
st.markdown("---")
st.markdown("Dashboard created with care for GymBeam Analytics | Data updated: " + 
           pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))

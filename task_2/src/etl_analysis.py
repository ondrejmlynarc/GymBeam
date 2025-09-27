# etl_analysis.py
import polars as pl
import urllib.request
import zipfile
import io
import os
from itertools import combinations
import numpy as np
from prefect import task, flow
from collections import Counter

# -----------------------------
# 1. Download postal codes
# -----------------------------
def download_postal_codes_github(url: str, country_code: str) -> pl.DataFrame:
    response = urllib.request.urlopen(url)
    zip_data = response.read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise ValueError(f"No CSV found in ZIP for {country_code}")
        with z.open(csv_files[0]) as f:
            df = pl.read_csv(f)

    df = df.select(['zipcode', 'place', 'latitude', 'longitude'])
    df = df.with_columns([
        pl.col('zipcode').cast(pl.Utf8).str.replace_all(" ", "").str.zfill(5).alias('postal_code'),
        pl.col('place').fill_null('Unknown').alias('place_name'),
        pl.col('latitude').cast(pl.Float64),
        pl.col('longitude').cast(pl.Float64),
        pl.lit(country_code).alias('country_code')
    ])

    df_unique = df.group_by('postal_code').agg([
        pl.first('place_name'),
        pl.first('latitude'),
        pl.first('longitude'),
        pl.first('country_code')
    ])
    return df_unique

# -----------------------------
# 2. Prefect ETL tasks
# -----------------------------
@task
def load_clean_orders_items(orders_path: str, items_path: str, output_dir: str):
    orders = pl.read_csv(orders_path)
    items = pl.read_csv(items_path)

    # normalize column names
    orders = orders.rename({c: c.strip().lower().replace(" ", "_") for c in orders.columns})
    items = items.rename({c: c.strip().lower().replace(" ", "_") for c in items.columns})

    # cast numeric columns
    items = items.with_columns([
        pl.col('product_price_local_currency').cast(pl.Float64),
        pl.col('sold_qty').cast(pl.Float64),
        pl.col('product_cost_eur').cast(pl.Float64)
    ])

    os.makedirs(output_dir, exist_ok=True)
    orders.write_csv(f"{output_dir}/orders_cleaned.csv")
    items.write_csv(f"{output_dir}/items_cleaned.csv")
    return orders, items

@task
def calculate_order_values(orders: pl.DataFrame, items: pl.DataFrame):
    items = items.with_columns((pl.col('product_price_local_currency') * pl.col('sold_qty')).alias('line_total'))
    order_totals = items.group_by('fk_sales_order').agg([pl.sum('line_total').alias('order_value')])
    enriched_orders = orders.join(order_totals, left_on='pk_sales_order', right_on='fk_sales_order', how='left')
    return enriched_orders

@task
def enrich_orders_with_cities(orders: pl.DataFrame, postal_df: pl.DataFrame, output_dir: str):
    orders = orders.with_columns(
        pl.col('postal_code').cast(pl.Utf8).str.replace_all(r"\.0$", "").str.zfill(5)
    )

    enriched = orders.join(postal_df, on=['postal_code', 'country_code'], how='left')
    enriched = enriched.with_columns(pl.col('place_name').fill_null('Unknown'))

    # normalize city names
    major_cities = ["Bratislava", "Košice", "Praha", "Brno", "Budapest"]
    for city in major_cities:
        enriched = enriched.with_columns(
            pl.when(pl.col("place_name").str.to_lowercase().str.starts_with(city.lower()))
            .then(pl.lit(city))
            .otherwise(pl.col("place_name"))
            .alias("place_name")
        )

    os.makedirs(output_dir, exist_ok=True)
    enriched.write_csv(f"{output_dir}/orders_enriched.csv")
    return enriched

# -----------------------------
# 3. Top 5 store candidates
# -----------------------------
@task
def top_5_store_candidates(orders: pl.DataFrame, output_dir: str):
    city_sales = orders.group_by(['place_name', 'latitude', 'longitude']).agg([
        pl.sum('order_value').alias('total_sales')
    ])

    existing_stores = orders.filter(pl.col('place_name').is_in(['Košice','Budapest','Praha'])) \
                            .group_by('place_name').agg([
                                pl.first('latitude').alias('latitude'),
                                pl.first('longitude').alias('longitude')
                            ]).rename({'place_name':'store'})

    # Haversine distance calculation
    city_lat = city_sales['latitude'].to_numpy()
    city_lon = city_sales['longitude'].to_numpy()
    store_lat = existing_stores['latitude'].to_numpy()
    store_lon = existing_stores['longitude'].to_numpy()

    R = 6371
    min_distances = []
    for i in range(len(city_lat)):
        lat1, lon1 = np.radians(city_lat[i]), np.radians(city_lon[i])
        lat2, lon2 = np.radians(store_lat), np.radians(store_lon)
        dphi = lat2 - lat1
        dlambda = lon2 - lon1
        a = np.sin(dphi/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlambda/2)**2
        distances = 2*R*np.arcsin(np.sqrt(a))
        min_distances.append(distances.min())

    city_sales = city_sales.with_columns(pl.Series(name='min_distance_km', values=min_distances))
    top5 = city_sales.filter(pl.col('min_distance_km') > 50)
    top5 = top5.sort(['total_sales', 'min_distance_km'], descending=[True, True]).head(5)
    top5.write_csv(f"{output_dir}/top_5_city_recommendations.csv")
    return top5

# -----------------------------
# 4. Top 10 product pairs
# -----------------------------

@task
def top_10_product_pairs(items: pl.DataFrame, orders: pl.DataFrame, output_dir: str):
    items_filtered = items.filter(
        ~((pl.col('product_price_local_currency') == 0) & (pl.col('product_cost_eur') > 0))
    )

    df = items_filtered.join(
        orders.select(['pk_sales_order']),
        left_on='fk_sales_order',
        right_on='pk_sales_order',
        how='inner'
    )

    df = df.with_columns(
        ((pl.col('product_price_local_currency') - pl.col('product_cost_eur')) * pl.col('sold_qty')).alias('margin')
    )

    top_pairs = Counter()
    grouped = df.group_by('fk_sales_order').agg(pl.col('fk_item').alias('products'))

    for row in grouped.to_dicts():
        products = [str(p) for p in row['products'] if p is not None]
        if len(products) > 1:
            pairs = combinations(sorted(products), 2)
            top_pairs.update(pairs)

    total_orders = df['fk_sales_order'].n_unique()
    top_10_pairs = top_pairs.most_common(10)
    top_10_pairs_percent = [(pair, count, count/total_orders*100) for pair, count in top_10_pairs]

    if top_10_pairs_percent:
        top_pairs_df = pl.DataFrame({
            'product_1': [pair[0] for pair, count, percent in top_10_pairs_percent],
            'product_2': [pair[1] for pair, count, percent in top_10_pairs_percent],
            'count': [count for pair, count, percent in top_10_pairs_percent],
            'percent_of_orders': [percent for pair, count, percent in top_10_pairs_percent]
        })
    else:
        top_pairs_df = pl.DataFrame({
            'product_1': [],
            'product_2': [],
            'count': [],
            'percent_of_orders': []
        })

    os.makedirs(output_dir, exist_ok=True)
    top_pairs_df.write_csv(f"{output_dir}/top_10_product_pairs.csv")
    return top_pairs_df

# -----------------------------
# 5. Monthly product margin
# -----------------------------
@task
def monthly_product_margin(items: pl.DataFrame, orders: pl.DataFrame, output_dir: str):
    df = items.join(orders.select(['pk_sales_order','created_at']), left_on='fk_sales_order', right_on='pk_sales_order', how='inner')
    df = df.filter(pl.col('fk_item').is_not_null())
    df = df.with_columns(((pl.col('product_price_local_currency') - pl.col('product_cost_eur')) * pl.col('sold_qty')).alias('margin'))
    df = df.with_columns(pl.col('created_at').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"))
    df = df.with_columns((pl.col('created_at').dt.truncate("1mo")).alias('year_month'))

    monthly_margin = df.group_by(['fk_item','year_month']).agg([
        pl.mean('margin').alias('avg_margin')
    ]).sort(['fk_item','year_month'])
    monthly_margin.write_csv(f"{output_dir}/monthly_product_margin.csv")
    return monthly_margin

# -----------------------------
# 6. Prefect ETL flow
# -----------------------------
@flow
def gymbeam_etl_flow(
    orders_path="../data/in/sales_order.csv", 
    items_path="../data/in/sales_order_item.csv", 
    output_dir="../data/out"
):
    """
    Main ETL pipeline for GymBeam sales data.
    Loads, cleans, enriches and analyzes sales data.
    Saves all results to the 'data/out' directory.
    """
    postal_urls = {
        "SK":"https://github.com/zauberware/postal-codes-json-xml-csv/raw/master/data/SK.zip",
        "CZ":"https://github.com/zauberware/postal-codes-json-xml-csv/raw/master/data/CZ.zip",
        "HU":"https://github.com/zauberware/postal-codes-json-xml-csv/raw/master/data/HU.zip"
    }

    postal_dfs = [download_postal_codes_github(url, c) for c, url in postal_urls.items()]
    postal_df = pl.concat(postal_dfs)

    orders, items = load_clean_orders_items(orders_path, items_path, output_dir)
    orders = calculate_order_values(orders, items)
    orders = enrich_orders_with_cities(orders, postal_df, output_dir)
    top5 = top_5_store_candidates(orders, output_dir)
    top_pairs = top_10_product_pairs(items, orders, output_dir)
    monthly_margin = monthly_product_margin(items, orders, output_dir)

    print("ETL flow completed successfully. Files are now in the 'data/out' directory.")
    return orders, top5, top_pairs, monthly_margin

if __name__=="__main__":
    gymbeam_etl_flow()

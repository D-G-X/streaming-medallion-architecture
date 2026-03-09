from sqlalchemy import text
from consumer.db import engine

CREATE_VIEW_SQL = """
CREATE OR REPLACE VIEW crypto_market_metrics AS
SELECT 
    date_trunc('minute', timestamp::timestamp) AS metric_minute,
    coin,
    ROUND(AVG(price_usd)::numeric, 2) AS avg_price,
    MAX(price_usd) AS max_price,
    MIN(price_usd) AS min_price,
    COUNT(*) AS data_points
FROM crypto_market_data
GROUP BY metric_minute, coin
ORDER BY metric_minute DESC, coin;
"""

def setup_view():
    print("Connecting to PostgreSQL...")
    try:
        with engine.begin() as connection:
            connection.execute(text(CREATE_VIEW_SQL))
        print("Successfully created view: crypto_market_metrics")
    except Exception as e:
        print(f"Error creating view: {e}")

if __name__ == "__main__":
    setup_view()
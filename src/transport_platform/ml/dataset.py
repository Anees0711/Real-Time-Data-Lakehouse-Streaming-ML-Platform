import snowflake.connector
import pandas as pd


def load_dataset():
    conn = snowflake.connector.connect(
        user="anees0711",
        password="Revenged@12345",
        account="YDQTCNC-IW80910",
        warehouse="COMPUTE_WH",
        database="FR_TRANSPORT",
        schema="PUBLIC",
    )

    query = """
        SELECT
            STATION,
            AVG_DELAY,
            NUM_EVENTS
        FROM STATION_METRICS_GOLD
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print("Dataset loaded:", df.shape)
    return df

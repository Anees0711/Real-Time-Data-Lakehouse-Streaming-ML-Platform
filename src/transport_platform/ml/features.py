def build_features(df):
    X = df[["NUM_EVENTS"]]
    y = df["AVG_DELAY"]

    print("Features ready:", X.shape)
    return X, y

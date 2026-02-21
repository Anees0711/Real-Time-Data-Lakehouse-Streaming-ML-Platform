from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error


def train_model(X, y):
    model = LinearRegression()
    model.fit(X, y)

    preds = model.predict(X)
    mae = mean_absolute_error(y, preds)

    print("Training complete. MAE:", mae)

    return model, mae

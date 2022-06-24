import pandas as pd
import numpy as np
import joblib
from fbprophet import Prophet
import requests


def predict(url):
    m = joblib.load('model.pkl')
    future = m.make_future_dataframe(periods=100, freq='H')
    forecast = m.predict(future)
    df = pd.read_csv("batch.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').resample('H').mean().reset_index()
    df = df.reset_index()[['timestamp', 'value']].rename({'timestamp': 'ds', 'value': 'y'}, axis = 'columns')
    result = pd.concat([df.set_index('ds')['y'], forecast.set_index('ds')[['yhat','yhat_lower','yhat_upper']]], axis=1)
    result['error'] = result['y'] - result['yhat']
    result['uncertainty'] = result['yhat_upper'] - result['yhat_lower']
    result['anomaly'] = result.apply(lambda x: 'Yes' if(np.abs(x['error']) > 1.5*x['uncertainty']) else 'No', axis = 1)
    newdf = result.loc[result['anomaly'] == 'Yes']
    for index, row in newdf.iterrows():
        msg = f'Found anomaly at time "{index}" with usage of "{row["y"]}" bytes'
        print("Message:")
        headers = {
            'Content-type': 'application/json',
        }
        json_data = {
            'text': msg,
        }
        response = requests.post(url, headers=headers, json=json_data)

# -*- coding:utf8 -*-

__author__ = 'Mx'

import pandas as pd
import numpy as np

from fbprophet import Prophet


def test01():
    df = pd.read_csv('./examples/example_wp_peyton_manning.csv')
    df['y'] = np.log(df['y'])
    print df.head()
    m = Prophet()
    m.fit(df);

    future = m.make_future_dataframe(periods=365)
    print future.tail()


    forecast = m.predict(future)
    print forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()

    # fig = m.plot(forecast);
    m.plot_components(forecast);




if __name__ == '__main__':
    #中文注释
    print 'ok'
    test01()


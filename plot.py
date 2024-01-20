import numpy as np
import pandas as pd
import plotly.express as px

# Read data
results = pd.read_csv('./examples/sim_result.csv', parse_dates = ['date_time_placed'], dtype = {'market_id':str})
# calculate and display cumulative pnl
results = results.sort_values(by = ['date_time_placed'])
results['cum_profit'] = results['profit'].cumsum()
px.line(results, 'date_time_placed', 'cum_profit').show()
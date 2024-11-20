#!/usr/bin/env python
# coding: utf-8

# ## WFM Forecasting and Scheduling Assistant

# The aim of this exercise is to implement a solution for workforce management by finding a workforce optimization tool at the contact centers of the Countries across the Caribbean region. The exercise is done by creating a forecast for a time series of hourly incoming call data by creating a model using TensorFlow (which was an experimental exercise that would've been developed on further) as well as the Prophet package which specializes in this assignment framework. The forecast was then used as input to an erlangC model which had its parameters tuned to the specific business requirements.

# Loading associated packages

# In[273]:


import pandas as pd
import os
from openpyxl import load_workbook
import numpy as np
import datetime
import dateutil.parser
import dateutil.parser
import shutil
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.kernel_ridge import KernelRidge
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import math
import dask.array as da
from prophet import Prophet
from datetime import datetime, timedelta
from pyworkforce.queuing import ErlangC


# #### Reading file from path.

# In[274]:


directory = r'C:\Users\roy_shaw\Artificial_Directory\WFM\New\OECS\60-min_interval'


# Folder of subfolders would easily be navigated by iterating through the directory. Using the os.walk function documents ending with the '.xlsx' extension were selected, loaded and merged into one dataframe repository.

# In[275]:


merged_df = pd.DataFrame()
for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith('.xlsx'):
            file_path = os.path.join(root, file)
            df = pd.read_excel(file_path, engine='openpyxl', header=1, date_format = '%Y-%m-%d %I:%M:%S %p')
            merged_df = pd.concat([merged_df, df], axis = 0, ignore_index=True)


# In[276]:


merged_df_ = merged_df.copy(deep=True)


# In[277]:


merged_df_.columns


# Assigning the labels for the associated columns.

# In[278]:


merged_df_.columns = ['Skills', 'Interval Start Time', 'Interval End Time', 'CSQ Name', 'Calls Presented', 'Avg Queue Time', 'Max Queue Time',                     'Calls Handled', 'Avg Handle Time', 'Max Handle Time', 'Calls Abandoned', 'Avg Queue Time', 'Max QueueTime', 'Service Level']


# Checking the dataframe.

# In[281]:


merged_df_


# Converting the 'Interval Start Time' column to a datetime column for python readability.

# In[282]:


merged_df_['Interval Start Time'] = pd.to_datetime(merged_df_['Interval Start Time'], format='mixed', infer_datetime_format=True)


# Data cleaning by removing the 'Skills' column, dropping NA values from the 'Interval Start Time' column and sorting same. At the end we see the output of the last 5 rows to check that the summary data that was present in the input data was removed (after dropping the NA values as specified).

# In[283]:


merged_df_.drop(labels=['Skills'], axis=1, inplace=True)


# In[284]:


merged_df_.dropna(how='any', subset='Interval Start Time', inplace=True)


# In[285]:


merged_df_.sort_values(by='Interval Start Time', inplace=True)


# In[286]:


merged_df_.tail()


# Selecting the columns of interest 'Interval Start Time', 'Interval End Time', 'Calls Presented', 'Calls Handled', 'Avg Handle Time', 'Service Level' from the dataframe.

# In[287]:


merged_df_ = merged_df_[['Interval Start Time', 'Interval End Time', 'Calls Presented', 'Calls Handled', 'Avg Handle Time', 'Service Level']]


# Second sweep through for cleaning and converting time format for the 'Interval Start Time' column to the date time structure with the 24hr format.

# In[288]:


merged_df_['Interval Start Time'].dropna(how='any', inplace=True)


# In[289]:


merged_df_['Interval Start Time'] = pd.to_datetime(merged_df_['Interval Start Time'], dayfirst=False, format='%m/%d/%y %I:%M:%S %p')


# In[290]:


merged_df_['Interval Start Time']=merged_df_['Interval Start Time'].dt.strftime('%Y-%m-%d %H:%M:%S')


# In[293]:


merged_df_['Interval Start Time']


# We convert the 'Interval Start Time' and 'Interval End Time' to a string and access the start and end times using string slicing. To add these features to the dataframe we create a list from the lst_strt and lst_end series that were the result of the slices and create the columns in the dataframe by reading the lists into the required columns.

# In[296]:


get_ipython().run_cell_magic('time', '', "merged_df_['Interval Start Time'] = pd.to_datetime(merged_df_['Interval Start Time'], infer_datetime_format=True, format='mixed')\nmerged_df_['Interval End Time'] = pd.to_datetime(merged_df_['Interval End Time'], infer_datetime_format=True, format='mixed')\nmerged_df_['trans_start'] = merged_df_['Interval Start Time'].dt.strftime('%d-%m-%Y %H:%M:%S')\nmerged_df_['trans_end'] = merged_df_['Interval End Time'].dt.strftime('%d-%m-%Y %H:%M:%S')\nmerged_df_['date'] = pd.to_datetime(merged_df_['Interval Start Time']).dt.date\n\nlst_strt = merged_df_['Interval Start Time'].dt.strftime('%d-%m-%Y %H:%M:%S').str[11:]\nlst_end = merged_df_['Interval End Time'].dt.strftime('%d-%m-%Y %H:%M:%S').str[11:]\n\ntrans_start = lst_strt.tolist()\ntrans_end = lst_end.tolist()\n\nmerged_df_['Interval Start Time'] = trans_start\nmerged_df_['Interval End Time'] = trans_end")


# In[298]:


filler_4 = merged_df_.copy(deep = True)


# In[299]:


filler_4 = filler_4.sort_values(by='Interval Start Time')


# In[303]:


df_new = filler_4


# In[307]:


df_new['Interval Start Datetime'] = pd.to_datetime(df_new['Interval Start Time'], dayfirst=True)


# In[308]:


df_new['date'] = pd.to_datetime(df_new['Interval Start Datetime'], dayfirst=True, format='mixed').dt.date


# In[309]:


df_new['Interval Start Time'] = df_new['Interval Start Datetime'].apply(lambda x: x.time())


# In[310]:


df_new['Day of week'] = df_new['Interval Start Datetime'].dt.day_name()


# In[311]:


df_new.to_csv(r'C:\Users\roy_shaw\Artificial_Directory\WFM\outputs\df_update_Nov-14_FY25_OECS.csv')


# ### Contact Centre Daily Forecast - Analysis Alpha Hourly Decomposition (w/TensorFlow)

# In[313]:


df_new.reset_index(inplace=True)


# In[314]:


result = df_new.copy(deep=True)


# In[315]:


result = pd.DataFrame(result).reset_index()


# The TensorFlow package requires numerical inputs hence the conversion here for the datetime information here where we further breakdown the day of the week and hour values using the sine and cosine functions based on the standard practice for this information.

# In[149]:


result['date'] = pd.to_datetime(df_new['date'])

result['year'] = result['date'].dt.year
result['month'] = result['date'].dt.month
result['day_of_week'] = result['date'].dt.dayofweek
result['hour'] = result['date'].dt.hour
result['minute'] = result['date'].dt.month

result['hour_sin'] = np.sin(2 * np.pi * result['hour'] / 48)
result['hour_cos'] = np.cos(2 * np.pi * result['hour'] / 48)

result['day_of_week_sin'] = np.sin(2 * np.pi * result['day_of_week'] / 7)
result['day_of_week_cos'] = np.cos(2 * np.pi * result['day_of_week'] / 7)

result['month_sin'] = np.sin(2 * np.pi * result['month'] / 12)
result['month_cos'] = np.cos(2 * np.pi * result['month'] / 12)

result.drop(columns=['date'], inplace = True)


# Training data using TensorFlow

# In[150]:


X = result[['year','month_sin','month_cos','day_of_week_sin','day_of_week_cos','hour_sin','hour_cos']]
y = result['Calls Presented']

features = X.astype('float32')
target = y.astype('float32')


# Here we split the data into training and testing based on the industry standard of 80% training and 20% testing.

# In[151]:


from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, shuffle=False)


# Encorporating features and requirements for TensorFlow processing and model development.

# In[152]:


# Convert to TensorFlow dataset
dataset = tf.data.Dataset.from_tensor_slices((X_train.values, y_train.values))

# Batch, shuffle, and prefetch the dataset for efficient training
dataset = dataset.batch(32).shuffle(1000).prefetch(tf.data.experimental.AUTOTUNE)

# Use the dataset for model training
model.fit(dataset, epochs=10)


# In[153]:


y_pred = model.predict(X_test)


# Exceptionally well value for MSE value derived from the predicted and observed values from the model.

# In[156]:


from sklearn.metrics import mean_squared_error

mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")


# Pickling model

# In[158]:


import pickle

with open("model.pkl", "wb") as Tensor_WFM:
    pickle.dump(model, Tensor_WFM)


# In[675]:


merged_df_.columns


# In[676]:


merged_df1 = merged_df_.copy()


# In[677]:


merged_df1 = merged_df1.groupby('Interval Start Time').agg({'Calls Presented':'sum', 'Service Level': 'mean'})


# In[678]:


merged_df1.to_csv(r'C:\Users\roy_shaw\Artificial_Directory\WFM\proof_of_concept_OECS.csv')


# The TensorFlow forecast with the model was due for development however the exercise was only just implemented before my post at the company was made redundant. The Prophet model used was simpler as the input parameters for the TensorFlow modeling required special attention and detail to ensre that the numerical elements were enabled and to convert to the original datetime structure which was used from the data cleaning stage (also for added readability) would require additional steps to be recaptured from the forecast from the TensorFlow model, given that the project was required with a level of timeliness this extra work was left in limbo in favor of the simpler Prophet package.

# #### Daily-Hourly Cadence Forecasts

# In[316]:


filler_comp = result.copy(deep=True)


# In[317]:


filler_comp['Interval Start Datetime'] = pd.to_datetime(filler_comp['Interval Start Datetime'], infer_datetime_format = True, format='mixed')


# In[318]:


filler_comp = filler_comp.groupby('Interval Start Datetime')['Calls Presented'].sum().reset_index()


# In[319]:


filler_comp = filler_comp.rename(columns = {'Interval Start Datetime':'ds', 'Calls Presented':'y'})


# In[320]:


filler_comp = filler_comp.set_index('ds')


# In[321]:


filler_comp.reset_index(inplace=True)


# After converting the Interval Start Time parameter to a string we use a reference datetime variable for the train-test boundary we then run the portion of the time series up to the reference point as the training data for the Prophet model, conversely the datetimes after the reference point are to be used to test the model.

# In[ ]:


split_date = '2024-06-30 17:30:00'
date_format = "%Y-%m-%d %I:%M:%S"


# In[ ]:


from datetime import datetime

split_date = '2024-06-30'
date_format = "%Y-%m-%d"
datetime_obj = datetime.strptime(split_date, date_format)
timestamp = pd.Timestamp(datetime_obj)
result_train = filler_comp.loc[filler_comp['ds'] <= timestamp].copy()
result_test = filler_comp.loc[filler_comp['ds'] > timestamp].copy()


# In[ ]:


model = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model.add_country_holidays(country_name='JM')
model.fit(result_train)


# In[161]:


result_test_fcst = model.predict(df=result_test.reset_index()                                    .rename(columns={'TEST SET':'y'}))


# In[ ]:


result_train = result_train.reset_index()     .rename(columns={'TRAINING SET':'y'}).tail()


# In[ ]:


result_test = result_test.reset_index()     .rename(columns={'TRAINING SET':'y'}).tail()


# #### Training data using FBProphet package

# Forcasting from training data

# In[86]:


f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
fig = model.plot(result_test_fcst,
                 ax=ax)
plt.show()


# Comparing Forecast with actual data

# In[426]:


f, ax = plt.subplots(1)
f.set_figheight(5)
f.set_figwidth(15)
ax.scatter(result_test['ds'], result_test['y'], color='r')
fig = model.plot(result_test_fcst, ax=ax)


# Error Metrics, we observe a favorable model to forecast and hence will proceed to do so.

# In[116]:


MSE = mean_squared_error(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])


# In[117]:


RMSE = math.sqrt(MSE)
RMSE


# In[118]:


R_square = r2_score(y_true=result_test['y'],
                   y_pred=result_test_fcst['yhat'])


# In[119]:


R_square


# In[120]:


def mape(y_test, pred):
    y_test, pred = np.array(y_test), np.array(pred)
    mape = np.mean(np.abs((y_test - pred) / y_test))
    return mape


# Forcasting future date values (Hour intervals). We create the model, add the region for which we want to track holidays in the calendar to better tune the model and fit the model to the 'filler_comp' dataframe.

# In[323]:


model_ = Prophet(weekly_seasonality=True, daily_seasonality=True, interval_width=0.95)
model_.add_country_holidays(country_name='LC')
model_.fit(filler_comp)


# Here we extend the model to the forecast time period and assign the forecast to an aptly named variable.

# In[324]:


future = model_.make_future_dataframe(periods=1440, freq = '60 min')
forecast = model_.predict(future)


# Converting the datetime values in the forecast output to a time value and storing as a column in the dataframe.

# In[325]:


get_ipython().run_cell_magic('time', '', "lst_input = list(forecast['ds'])\ntrans_start = []\ntrans_end = []\ntrans_date = []\nfor i in range(len(lst_input)):\n    df_trans_start = pd.to_datetime(forecast['ds'])\n    trans_start.append(df_trans_start.iloc[i].strftime('%H:%M %p'))\nforecast['Time_period'] = trans_start")


# Removing extraneous rows for calls that were received outside of the contact centre's operating hours.

# In[326]:


working_hrs_start = ['08:00 AM', '09:00 AM', '10:00 AM', '11:00 AM', '12:00 PM', '13:00 PM', '14:00 PM', '15:00 PM', '16:00 PM', '17:00 PM', '18:00 PM']
df_new = forecast[forecast['Time_period'].isin(working_hrs_start)]


# In[327]:


erLang = df_new.copy()


# In[329]:


erLang


# In[330]:


erLang['ds']


# Replacing forecasts for Sundays, which show values less than 1 as one (1) and can be identified in the resulting dataframe.

# In[331]:


erLang['yhat'][erLang['yhat'] <= 0] = 1


# For the erlangC calculator we require a single-column input that we will use the forecasted value obtained as the input by selecting the corresponding 'yhat' variable.

# In[332]:


erlC_forecast = list(erLang['yhat'])


# In[333]:


erlC_forecast = pd.DataFrame(erlC_forecast)


# In[334]:


erlC_forecast.to_csv(r'C:\Users\Artificial_Directory\WFM\forecast_erLang_Nov-14_FY25_OECS.csv')


# Converting from a pandas dataframe to a numpy array. We use the numpy array as input for the erlangC calculator from the python pyworkforce package.

# In[335]:


erlC_forecast = erlC_forecast.squeeze()


# In[336]:


from pyworkforce.queuing import MultiErlangC

param_grid = {"transactions": erlC_forecast, "aht": [4], "interval": [60], "asa": [20 / 60], "shrinkage": [0.30]}
multi_erlang = MultiErlangC(param_grid=param_grid, n_jobs=-1)
required_positions_scenarios = {"service_level": [0.85], "max_occupancy": [0.65]}
positions_requirements = multi_erlang.required_positions(required_positions_scenarios)
print("positions_requirements: ", positions_requirements)


# Extracting the values in a readable dataframe format as the dictionary output is unfavorable for viewing. We then inpout the datetime value and forecast to add value to the exercise.

# In[337]:


df_WFM = pd.DataFrame.from_dict(positions_requirements)


# In[338]:


df_WFM['date_time'] = erLang['ds'].values
df_WFM['forecast'] = erLang['yhat'].values
df_WFM.to_csv(r'C:\Users\Artificial_Directory\WFM\outputs\WFM_erLang_forecast_Nov-14_OECS-FY25.csv')


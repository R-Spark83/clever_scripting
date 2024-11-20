#!/usr/bin/env python
# coding: utf-8

# ## eCommerce Report

# *The ecommerce report integrates data from several sources to provide one output with a consistent methodology required to put into detsil all the requirements that need to be met for accurate data capture and correctness of reporting on a weekly/monthly basis.*

# Calling all the required classes and packages for the construction of the report.

# In[173]:


import pandas as pd
from datetime import date, datetime
import numpy as np
import os
import pyodbc
import chardet
import xlrd
import requests
import json
import urllib
import os
import time


# Changing the directory to the folder containing a looker report export which contains the information regarding the "Completed At" field required for calculating the turn-around-time (TAT) for the transaction, which is a key metric regarding our operational fulfillment of deliveries given our commitment of a two (2) day delivery authorize and transfer of stock on eCommerce and in-store purchases.

# In[174]:


os.chdir(r'C:\Users\roy_shaw\Artificial_Path\eCommerce\Raw_Data\Inputs\FY-25\Weekly\Looker')


# Change input file here:

# In[175]:


df = pd.read_excel(r'week_32-33_complete_FY25.xlsx')


# In[176]:


df.rename(columns={'number_order':'ID', 'order_finish_dt':'Completed At'}, inplace=True)


# Removing extraneous rows from transactions which are outside of the scope of the Caribbean region.

# In[178]:


df = df[~df['buyer_country'].isin(['PARAGUAY', 'NICARAGUA', 'HONDURAS', 'GUATEMALA', 'EL SALVADOR', 'ECUADOR', 'COSTA RICA'])]


# In[179]:


df.reset_index(inplace=True)


# The structure of the report delivered by corporate is hardcoded in Spanish in a format which is unfriendly for computation as required, hence the given the text-based nature of the datetime stamp of the "Completed At" field we had to manipulate the string by slicing and concatenating the date-input to a usable format which is favorable to computation using date-differencing algebraic methods. 

# In[180]:


for i in range(len(df['ID'])):
    if (((type(df.loc[i, 'Completed At']) != int)|(type(df.loc[i, 'Completed At']) != float))&(type(df.loc[i, 'Completed At'])==str)):
        str_func = str(df.loc[i, 'Completed At'][-8:-5])
        if ("ene" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "01" + "/" + df['Completed At'][i][-4:]
        elif ("feb" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "02" + "/" + df['Completed At'][i][-4:]
        elif ("may" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "05" + "/" + df['Completed At'][i][-4:]
        elif ("dic" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "12" + "/" + df['Completed At'][i][-4:]
        elif ("mar" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "03" + "/" + df['Completed At'][i][-4:]
        elif ("abr" in str_func):
            df['Completed At'][i] = str(df['Completed At'][i][:-9]) + "/" + "04" + "/" + df['Completed At'][i][-4:]
        elif ("ago" in str_func):
            df.loc[i, 'Completed At'] = str(df.loc[i, 'Completed At'][:-9]) + "/" + "08" + "/" + df.loc[i, 'Completed At'][-4:]
        elif (type(df.loc[i, 'Completed At']==int)|(type(df.loc[i, 'Completed At']==float))):
            df.loc[i, 'Completed At'] = pd.to_datetime(df.loc[i, 'Completed At']).strftime(format='%d/%m/%Y')
        else:
            continue
    else:
        df.loc[i, 'Completed At'] = pd.to_datetime(df.loc[i, 'Completed At'], format = '%d/%m/%Y')


# The 'ID' column contains characters which aren't universal to all the business frameworks in that the string set contains more characters than which is to be counted to the left. Hence we will capture only the last 11 characters to the right as indicated by the below input.

# In[181]:


for i in range(len(df['index'])):
    df.loc[i, 'ID'] = int(df.loc[i, 'ID'][-11:])


# Changing the directory to capture a report derived from the sales tracker website Magento, this has all the information related to the transaction and has the most recent transaction fulfillment in real time. This report however misses the "Completed At" field and we will combine both reports to create an integrated file.

# In[182]:


os.chdir(r'C:\Users\roy_shaw\Artificial_Directory\eCommerce\Raw_Data\Inputs\FY-25\Weekly\Magento')


# In[186]:


df_ = pd.read_excel(r'Nov_week_1_FY25.xlsx')


# Here we repeat a similar criteria of using the universal customer 'ID' for consistency in our tracking of the transactions.

# In[187]:


for i in range(len(df_['ID'])):
    df_.loc[i, 'ID'] = int(df_.loc[i, 'ID'][-11:])


# Here we merge both the dataframes from the Looker and Magento reports in order to pull the information together on the cleaned and universal 'ID' field to have one file capable of tracking the transactions and have the required 'Completed At' field.

# In[188]:


merged_df = df_.merge(df, on='ID')


# To ensure reporting accuracy should there be an instance of a duplication of a transaction when the report was pulled then we will drop the duplicated transaction and keep the first record.

# In[189]:


merged_df.drop_duplicates(subset='ID', keep='first', inplace=True)


# Checking the attributes of the dataframe for completeness.

# In[192]:


merged_df.columns


# In[193]:


df = merged_df.copy()


# Now that we have the transaction as well as the 'Completed At' information in one dataframe, we can now manipulate the data to meet the requirements of recency and to eliminate transactions which are fraudulent.
# 
# Below the 'Completed At' field, being a string was converted to a python datetime object to facilitate the requirement of the software to carry out a proper arithmetical difference between the purchase date and the completed at date for the TAT metric.

# In[194]:


df['Completed At'] = pd.to_datetime(df['Completed At'], dayfirst = True)


# In[195]:


df['Purchase Date'] = pd.to_datetime(df['Purchase Date']).dt.date


# For transactions which have not yet been completed we will split the dataframe into two, one branch of the split dataframe will have the completed transactions and the other will have the pending transactions. we create the df_empty dataframe to house the pending transactions and removing the pending transactions from the main dataframe to complete the split.

# In[201]:


df['Completed At'].replace('NaT','NaN')
df['Null_Bool'] = df['Completed At'].isnull()
df_empty = df[df['Null_Bool'] == True]
df.drop(df['Null_Bool'].index[df['Null_Bool'] == True], inplace = True)


# Conducting a routine data-cleaning in transforming the information in the records for the Country which the eCommerce transaction was recorded in both dataframes as shown below.

# In[205]:


df['Purchase Point'].replace(to_replace={'SC Antigua Website    SC Antigua Store        SC Antigua Store View': 'Antigua', 'SC Barbados Website    SC Barbados Store        SC Barbados Store View': 'Barbados',             'SC Belize Website    SC Belize Store        SC Belize Store View': 'Belize', 'SC Curacao Omni Website    SC Curacao Omni Store        SC Curacao Omni Store View': 'Curacao',              'SC Dominica Website    SC Dominica Store        SC Dominica Store View': 'Dominica',              'SC Grenada Website    SC Grenada Store        SC Grenada Store View': 'Grenada','SC Guyana Website    SC Guyana Store        SC Guyana Store View': 'Guyana',             'SC Jamaica Website    SC Jamaica Store        SC Jamaica Store View': 'Jamaica',              'SC St. Kitts and Nevis Website    SC St. Kitts and Nevis Store        SC St. Kitts and Nevis Store View': 'St. Kitts and Nevis',             'SC St. Lucia Website    SC St. Lucia Store        SC St. Lucia Store View': 'St. Lucia',             'SC St. Vincent Website    SC St. Vincent Store        SC St. Vincent Store View': 'St. Vincent',             'SC Trinidad and Tobago Website    SC Trinidad and Tobago Store        SC Trinidad and Tobago Store View': 'Trinidad and Tobago',             'SO Curacao Omni Website    SO Curacao Omni Store        SO Curacao Omni Store View': 'Curacao'}, inplace = True)


# In[206]:


df_empty['Purchase Point'].replace(to_replace={'SC Antigua Website    SC Antigua Store        SC Antigua Store View': 'Antigua', 'SC Barbados Website    SC Barbados Store        SC Barbados Store View': 'Barbados',             'SC Belize Website    SC Belize Store        SC Belize Store View': 'Belize', 'SC Curacao Omni Website    SC Curacao Omni Store        SC Curacao Omni Store View': 'Curacao',              'SC Dominica Website    SC Dominica Store        SC Dominica Store View': 'Dominica',              'SC Grenada Website    SC Grenada Store        SC Grenada Store View': 'Grenada','SC Guyana Website    SC Guyana Store        SC Guyana Store View': 'Guyana',             'SC Jamaica Website    SC Jamaica Store        SC Jamaica Store View': 'Jamaica',              'SC St. Kitts and Nevis Website    SC St. Kitts and Nevis Store        SC St. Kitts and Nevis Store View': 'St. Kitts and Nevis',             'SC St. Lucia Website    SC St. Lucia Store        SC St. Lucia Store View': 'St. Lucia',             'SC St. Vincent Website    SC St. Vincent Store        SC St. Vincent Store View': 'St. Vincent',             'SC Trinidad and Tobago Website    SC Trinidad and Tobago Store        SC Trinidad and Tobago Store View': 'Trinidad and Tobago',             'SO Curacao Omni Website    SO Curacao Omni Store        SO Curacao Omni Store View': 'Curacao'}, inplace = True)


# Also we clean the information relating to the 'Shipping Information' field here since both inputs shown below are indicative of cashloan payments made on the system and not necessarily of a new transaction we remove these records from both dataframes as per business requirements, since they are extraneous to the reporting criteria.

# In[207]:


df.drop(df.index[df['Shipping Information'] == 'Courts Carrier - Home delivery (Ready Finance payments have no fee)'], inplace = True)
df.drop(df.index[df['Shipping Information'] == 'Courts Carrier - Home Delivery Ready Finance  payments have no fee'], inplace = True)


# In[208]:


df_empty.drop(df_empty.index[df_empty['Shipping Information'] == 'Courts Carrier - Home delivery (Ready Finance payments have no fee)'], axis = 0, inplace = True)
df_empty.drop(df_empty.index[df_empty['Shipping Information'] == 'Courts Carrier - Home Delivery Ready Finance  payments have no fee'], axis = 0, inplace = True)


# The purchases made under the headliner of 'Store Pickup' captured information in the 'Shipping Information' attribute for the purchaser's name and store location requested for pickup, which would make it difficult to track the count of such shipments as each record would be unique in it's own and hence would be difficult to aggregate. The below procedure was used to remove the extraneous data and allow for a universal label for the 'Store Pickup' field.

# Below the algorithm works by instantiating a list object called str_, we then loop through the range of the 'Shipping Information' column, we then add each individual element in the column to the list we created initially. We then create a pandas series called is_store_pickup labelled 'Shipping Information' from the list of records derived from the list. The boolean variable series is_store_pickup is then ran which checks that the criteria that the string contains the key "Store Pickup" afterwhich we run a loop to replace those values in the dataframe column which contain the key with the correct value of "Store Pickup". Rectifying the issue.

# In[209]:


str_ = []
for i in range(0,len(df["Shipping Information"])):
    str_.append(df["Shipping Information"].values[i])
    i += 1
series_ = pd.Series(str_, name="Shipping Information")
is_store_pickup = series_.str.contains("Store Pickup")
for i in range(0,len(df["Shipping Information"])):
    if is_store_pickup[i] == True:
        df["Shipping Information"].replace(df["Shipping Information"].values[i],"Store Pickup", inplace = True)
        i += 1
    else:
        i += 1


# Here we replace the statuses, payment methods and shipping information to standard practice as is required for business acumen for both dataframes.

# In[210]:


df['Status'].replace(to_replace={'canceled':'Canceled', 'complete':'Complete', 'Cancel POD':'Canceled', 'cancel_pod':'Canceled', 'complete_pod':'Complete', 'closed': 'Closed', 'holded':'Holded', 'pending':'Pending', 'pending_pod':'Pending', 'Pending Payment':'Pending', 'Pending Payment':'Pending', 'Pending POD':'Pending', 'PendingRF':'Pending', 'processing':'Processing', 'processing_pending':'Processing', 'Processing POD':'Processing', 'refund':'Canceled', 'Refund':'Canceled', 'Cancelado':'Cancelled','Pago Pendiente':'Pending', 'Pendiente':'Pending', 'Procesando':'Processing'}, inplace = True)
df['Payment Method'].replace(to_replace={'paymentondelivery':'Pay on Delivery','fac_gateway':'Credit Card/Debit Card','payinstore':'Pay in Store','checkmo':'Ready Finance Application','free':'Gift Card','emma':'EMMA','courts_storecard':'Courts Storecard', 'summasolutions_facgateway':'Credit/Debit Card','cashondelivery':'Pay on Delivery','paypal_standard':'Paypal', 'fac_power_tranz':'Credit/Debit Card'}, inplace = True)
df["Shipping Information"].replace(to_replace={"Courts Carrier - Home delivery":"Courts Carrier", "Courts Carrier - FREE Home Delivery":"Courts Carrier", "Courts Carrier - Express Delivery":"Courts Carrier", "Courts Carrier - Home Delivery":"Courts Carrier","Free Shipping - Free delivery":"Free Shipping","Free Shipping - Free Shipping":"Free Shipping","Shipping Option - Free Shipping":"Free Shipping","Shipping Method - Express Delivery":"Courts Carrier",'Courts Carrier - Express PickUp - Incmplete':"Courts Carrier","Shipping Method - Free Delivery":"Free Delivery","Free Shipping - Free":"Free Delivery","Shipping Option - Express Pickup":"Store Pickup","Courts Carrier - FREE Home Delivery (*delivery by Christmas NOT guaranteed)":"Courts Carrier","Courts Carrier - Free Delivery":"Courts Carrier","Flat Rate - Home Delivery":"Flat Rate","Shipping Method - Home Delivery":"Free Shipping","Courts Carrier - FREE Delivery":"Free Shipping", "Courts Carrier - Express":"Courts Carrier", "Courts Carrier - Home - delivery": "Courts Carrier", "Courts Carrier - TT home delivery": "Courts Carrier"}, inplace = True)


# In[212]:


df_empty['Status'].replace(to_replace={'canceled':'Canceled', 'complete':'Complete', 'Cancel POD':'Canceled', 'cancel_pod':'Canceled', 'complete_pod':'Complete', 'closed': 'Closed', 'holded':'Holded', 'pending':'Pending', 'pending_pod':'Pending', 'Pending Payment':'Pending', 'Pending Payment':'Pending', 'Pending POD':'Pending', 'PendingRF':'Pending', 'processing':'Processing', 'processing_pending':'Processing', 'Processing POD':'Processing', 'refund':'Canceled', 'Refund':'Canceled', 'Cancelado':'Cancelled','Pago Pendiente':'Pending', 'Pendiente':'Pending', 'Procesando':'Processing'}, inplace = True)
df_empty['Payment Method'].replace(to_replace={'paymentondelivery':'Pay on Delivery','fac_gateway':'Credit Card/Debit Card','payinstore':'Pay in Store','checkmo':'Ready Finance Application','free':'Gift Card','emma':'EMMA','courts_storecard':'Courts Storecard', 'summasolutions_facgateway':'Credit/Debit Card','cashondelivery':'Pay on Delivery','paypal_standard':'Paypal', 'fac_power_tranz':'Credit/Debit Card'}, inplace = True)
df_empty["Shipping Information"].replace(to_replace={"Courts Carrier - Home delivery":"Courts Carrier", "Courts Carrier - FREE Home Delivery":"Courts Carrier", "Courts Carrier - Express Delivery":"Courts Carrier", "Courts Carrier - Home Delivery":"Courts Carrier","Free Shipping - Free delivery":"Free Shipping","Free Shipping - Free Shipping":"Free Shipping","Shipping Option - Free Shipping":"Free Shipping","Shipping Method - Express Delivery":"Courts Carrier",'Courts Carrier - Express PickUp - Incmplete':"Courts Carrier","Shipping Method - Free Delivery":"Free Delivery","Free Shipping - Free":"Free Delivery","Shipping Option - Express Pickup":"Store Pickup","Courts Carrier - FREE Home Delivery (*delivery by Christmas NOT guaranteed)":"Courts Carrier","Courts Carrier - Free Delivery":"Courts Carrier","Flat Rate - Home Delivery":"Flat Rate","Shipping Method - Home Delivery":"Free Shipping","Courts Carrier - FREE Delivery":"Free Shipping", "Courts Carrier - Express":"Courts Carrier", "Courts Carrier - Home - delivery": "Courts Carrier", "Courts Carrier - TT home delivery":"Courts Carrier"}, inplace = True)


# Doing a sweep through of the completed at field yet again to ensure the fullness of the data.

# In[214]:


df.dropna(axis = 0, how = 'any', subset = ['Completed At'], inplace = True)


# By business practice if the shipping information is blank it is a "Flat Rate" category we will address this issue for both dataframes.

# In[215]:


df["Shipping Information"].fillna("Flat Rate",inplace = True)


# In[216]:


df_empty["Shipping Information"].fillna("Flat Rate",inplace = True)


# Here we are changing directory again, the information is set back for a three month period to facilitate any pending transactions. The FAC (First Atlantic Commerce) report is used to trace back online transactions which are fraudulent, here we concatenate the past three months information as well as the weekly specific data and does not match the 'Approved' transactcions for the 'ID' column (which again here is based on the standard layout across the business).

# In[217]:


os.chdir(r'C:\Users\roy_shaw\Artificial_Directory\eCommerce\Raw_Data\FAC Draw downs\Weekly\FY25')
df_pymt_4 = pd.read_excel('FAC_week_32-33_FY25.xlsx', 'rpTransaction')

os.chdir(r'C:\Users\roy_shaw\Artificial_Directory\eCommerce\Raw_Data\FAC Draw downs\Monthly\FY25')
df_pymt_1 = pd.read_excel('FAC_Aug_FY25.xlsx', 'rpTransaction')
df_pymt_2 = pd.read_excel('FAC_Sep_FY25.xlsx', 'rpTransaction')
df_pymt_3 = pd.read_excel('FAC_Oct_FY25.xlsx', 'rpTransaction')


# In[218]:


df_prep = pd.concat([df_pymt_1, df_pymt_2, df_pymt_3, df_pymt_4])


# In[219]:


df_prep['Order ID'] = df_prep['Order ID'][-11:]


# In[220]:


df_filtered = df_prep[df_prep['Response Description'] != 'Approved']


# Here the column is created 'Corrected_Bool' which matches the dataframe df_prep as the concatenated dataframe of the FAC transactions, the df_filtered dataframe is a dataframe of the transactions which are not 'Approved' and matching these dataframes creates a boolean value which we use to remove the fraudlent or erroneously repeated transactions.

# In[221]:


df['Corrected_Bool'] = df['ID'].isin(df_filtered['Order ID'])
df.drop(df.index[df['Corrected_Bool'] == True], axis = 0, inplace = True)


# In[222]:


df_empty['Corrected_Bool'] = df_empty['ID'].isin(df_filtered['Order ID'])
df_empty.drop(df_empty.index[df_empty['Corrected_Bool'] == True], axis = 0, inplace = True)


# Creating a method to calculate the TAT from the dates of the report.

# In[224]:


from datetime import date
def get_difference(startdate, enddate):
    diff = enddate - startdate
    return diff.days


# Here we instantiate the empty list lst_. Calculating the TAT from the 'Purchase Date' and 'Completed At' dates here if the difference in date is zero, as per request from the eCommerce team we will impute this value as 1. Then we append the value of the difference to the lst_ list, afterwhich we create and set the 'TAT' column to be the lst_ list.

# In[225]:


lst_ = []
df.reset_index(inplace=True)
for i in range(len(df['Purchase Date'])):
    if get_difference(df['Purchase Date'].loc[i], df['Completed At'].loc[i]) == 0:
        lst_.append(1)
    else:
        lst_.append(get_difference(df['Purchase Date'].loc[i], df['Completed At'].loc[i]))
df['TAT'] = lst_


# We now combine the dataframes of the completed and pending transactions and we are ensuring that the 'Purchase Point' information is consistent for the Countries as above.

# In[228]:


new_df1 = pd.concat([df,df_empty])


# In[229]:


new_df1['Purchase Point'].replace(to_replace={'SC Jamaica Website   SC Jamaica Store      SC Jamaica Store View':'Jamaica',                                                                         'SC Barbados Website   SC Barbados Store      SC Barbados Store View':'Barbados',                                                                         'SC Guyana Website   SC Guyana Store      SC Guyana Store View':'Guyana',                                                                         'SC St. Kitts and Nevis Website   SC St. Kitts and Nevis Store      SC St. Kitts and Nevis Store View':'St. Kitts and Nevis',                                                                         'SC St. Lucia Website   SC St. Lucia Store      SC St. Lucia Store View':'St. Lucia',                                                                         'SC Dominica Website   SC Dominica Store      SC Dominica Store View':'Dominica',                                                                         'SC Trinidad and Tobago Website   SC Trinidad and Tobago Store      SC Trinidad and Tobago Store View':'Trinidad and Tobago',                                                                         'SC St. Vincent Website   SC St. Vincent Store      SC St. Vincent Store View':'St. Vincent',                                                                         'SC Antigua Website   SC Antigua Store      SC Antigua Store View':'Antigua',                                                                         'SC Belize Website   SC Belize Store      SC Belize Store View':'Belize',                                                                         'SC Grenada Website   SC Grenada Store      SC Grenada Store View':'Grenada',                                                                         'SO Curacao Omni Website   SO Curacao Omni Store      SO Curacao Omni Store View':'Curacao'}, inplace=True)


# Below we attempt to  convert the hard-coded and unfavorable string values of the format 'BD$XXXX.XX' into a more user-friendly format by again using sub-strings and slices.

# In[230]:


for i in range(len(new_df1['Purchase Point'])):
    new_df1.loc[i, 'Grand Total (Purchased)'] = str(new_df1.loc[i, 'Grand Total (Purchased)'])
    new_df1.loc[i, 'Subtotal'] = str(new_df1.loc[i, 'Subtotal'])
    new_df1.loc[i, 'Grand Total (Purchased)'] = new_df1.loc[i, 'Grand Total (Purchased)'][3:]
    new_df1.loc[i, 'Subtotal'] = new_df1.loc[i, 'Subtotal'][3:]


# In[231]:


for i in range(len(new_df1['Grand Total (Purchased)'])):
    new_df1.loc[i, 'Grand Total (Purchased)'] = new_df1.loc[i, 'Grand Total (Purchased)'].replace(",","")
    new_df1.loc[i, 'Grand Total (Purchased)'] = new_df1.loc[i, 'Grand Total (Purchased)'].replace("$","")
    new_df1.loc[i, 'Subtotal'] = new_df1.loc[i, 'Subtotal'].replace(",","")
    new_df1.loc[i, 'Subtotal'] = new_df1.loc[i, 'Subtotal'].replace("$","")


# In[232]:


for i in range(len(new_df1['Grand Total (Purchased)'])):
    if new_df1.loc[i, 'Grand Total (Purchased)'][:3] == ('XCD' or 'ANG'):
        new_df1.loc[i, 'Grand Total (Purchased)'] = float(new_df1.loc[i, 'Grand Total (Purchased)'][3:])
        new_df1.loc[i, 'Subtotal'] = float(new_df1.loc[i, 'Subtotal'][3:])
    elif (new_df1.loc[i, 'Grand Total (Purchased)'][:4] == 'Bds'):
        new_df1.loc[i, 'Grand Total (Purchased)'] = float(new_df1.loc[i, 'Grand Total (Purchased)'][4:])
        new_df1.loc[i, 'Subtotal'] = float(new_df1.loc[i, 'Subtotal'][4:])
    elif (new_df1.loc[i, 'Grand Total (Purchased)'][:1] == 'J'):
        new_df1.loc[i, 'Grand Total (Purchased)'] = float(new_df1.loc[i, 'Grand Total (Purchased)'][1:])
        new_df1.loc[i, 'Subtotal'] = float(new_df1.loc[i, 'Subtotal'][1:])
    elif new_df1.loc[i, 'Grand Total (Purchased)'] == ('TT' or 'GY' or'BZ'):
        new_df1.loc[i, 'Grand Total (Purchased)'] = float(new_df1.loc[i, 'Grand Total (Purchased)'][2:])
        new_df1.loc[i, 'Subtotal'] = float(new_df1.loc[i, 'Subtotal'][2:])
    elif new_df1.loc[i, 'Grand Total (Purchased)'] == '':
        new_df1.loc[i, 'Grand Total (Purchased)'] = 0


# The repeated code below is used to separate new purchases from payments on installment for existing ReadyFinance transactions. The records are then separated into different dataframes using the created 'is_RFpymt' column in the data_validation dataframe which is the combined dataframe of transactions filtered on by Country.

# #### Jamaica

# In[233]:


df_validation = new_df1[new_df1['Purchase Point'] == 'Jamaica']
lst_GT = list(df_validation['Grand Total (Purchased)'].astype('float'))
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%100 == 0):
        lst_validity.append('True')
    else:
        lst_validity.append('False')
df_validation['is_RFpymt'] = lst_validity


# In[234]:


new_df1.drop(new_df1.index[new_df1['Purchase Point'] == 'Jamaica'], axis = 0, inplace = True)


# In[235]:


count_RF = df_validation[df_validation['is_RFpymt'] != 'False']


# In[236]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] != 'False'], axis = 0, inplace = True)


# In[237]:


new_df2 = pd.concat([new_df1,df_validation])


# #### Barbados

# In[238]:


df_validation = new_df2[new_df2['Purchase Point'] == 'Barbados']
lst_GT = list(df_validation['Grand Total (Purchased)'].astype('float'))
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%2 == 0) & (lst_GT[i] <= 80):
        lst_validity.append('True')
    elif (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%5 == 0) & (lst_GT[i] > 80) & (lst_GT[i] <= 360):
        lst_validity.append('True')
    elif (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%10 == 0) & (lst_GT[i] > 360) & (lst_GT[i] <= 500):
        lst_validity.append('True')
    else:
        lst_validity.append('False')
df_validation['is_RFpymt'] = lst_validity


# In[239]:


new_df2.drop(new_df2.index[new_df2['Purchase Point'] == 'Barbados'], axis = 0, inplace = True)


# In[240]:


count_RF = df_validation[df_validation['is_RFpymt'] != 'False']


# In[241]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] != 'False'],axis = 0, inplace = True)


# In[242]:


new_df3 = pd.concat([new_df2,df_validation])


# #### Guyana

# In[243]:


df_validation = new_df3[new_df3['Purchase Point'] == 'Guyana']
lst_GT = list(df_validation['Grand Total (Purchased)'].astype('float'))
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%100 == 0):
        lst_validity.append('True')
    else:
        lst_validity.append('False')
df_validation['is_RFpymt'] = lst_validity


# In[244]:


new_df3.drop(new_df3.index[new_df3['Purchase Point'] == 'Guyana'], axis = 0, inplace = True)


# In[245]:


count_RF = df_validation[df_validation['is_RFpymt'] != 'False']


# In[246]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] != 'False'],axis = 0, inplace = True)


# In[247]:


new_df4 = pd.concat([new_df3,df_validation])


# #### OECS

# In[248]:


df_validation = new_df4[(new_df4['Purchase Point'] == 'St. Lucia') | (new_df4['Purchase Point'] == 'St. Vincent') |                       (new_df4['Purchase Point'] == 'St. Kitts and Nevis') | (new_df4['Purchase Point'] == 'Antigua') |                       (new_df4['Purchase Point'] == 'Grenada') | (new_df4['Purchase Point'] == 'Dominica')]
lst_GT = list(df_validation['Grand Total (Purchased)'].astype('float'))
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%10 == 0) & (lst_GT[i] <= 3000):
        lst_validity.append('True')
    else:
        lst_validity.append('False')
df_validation['is_RFpymt'] = lst_validity


# In[249]:


new_df4.drop(new_df4.index[(new_df4['Purchase Point'] == 'St. Lucia')|(new_df4['Purchase Point'] == 'St. Vincent') |                          (new_df4['Purchase Point'] == 'St. Kitts and Nevis') | (new_df4['Purchase Point'] == 'Antigua') |                          (new_df4['Purchase Point'] == 'Grenada') | (new_df4['Purchase Point'] == 'Dominica')]             , axis = 0, inplace = True)


# In[250]:


count_RF = df_validation[df_validation['is_RFpymt'] != 'False']


# In[251]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] != 'False'],axis = 0, inplace = True)


# In[252]:


new_df5 = pd.concat([new_df4,df_validation])


# #### Belize

# In[253]:


df_validation = new_df5[new_df5['Purchase Point'] == 'Belize']
lst_GT = list(df_validation['Grand Total (Purchased)'].astype('float'))
lst_ST = list(df_validation['Subtotal'])
lst_validity = []
for i in range(len(lst_GT)):
    if (lst_GT[i] == lst_ST[i]) & (lst_GT[i]%5 == 0) & (lst_GT[i] <= 750):
        lst_validity.append('True')
    else:
        lst_validity.append('False')
df_validation['is_RFpymt'] = lst_validity


# In[254]:


new_df5.drop(new_df5.index[new_df5['Purchase Point'] == 'Belize'], axis = 0, inplace = True)


# In[255]:


count_RF = df_validation[df_validation['is_RFpymt'] != 'False']


# In[256]:


df_validation.drop(df_validation.index[df_validation['is_RFpymt'] != 'False'],axis = 0, inplace = True)


# In[257]:


new_df6 = pd.concat([new_df5,df_validation])


# In[258]:


columns_to_drop = [col for col in df.columns if 'Unnamed' in col]
new_df6.drop(columns=columns_to_drop, inplace=True)


# Exporting dataframe to an excel file for visualization in PowerBI.

# In[259]:


new_df6.to_csv(r'C:\Users\roy_shaw\Artificial_Directory\eCommerce\Coding output\Weekly\FY25\week_32-33_ecom.csv', index = False, encoding = 'utf-8')


# In[260]:


count_RF.to_csv(r'C:\Users\roy_shaw\Artificial_Directory\eCommerce\Coding output\Monthly\FY25\RF_week_32-33_FY25_ecom.csv', index = False, encoding = 'utf-8')


# As part of the normal operational revenue tracking I was assigned the task by management to determine from the weekly/monthly produced ecommerce report a breakout of the revenue for online transactions by channel and country, which led to the development of this script. The primary function of which was complimentary to the ecommerce report for what was known at the time as the alternative channel report the associated method for the alternative channel report is also on the reporsitory and can be explored under the file by the same name.

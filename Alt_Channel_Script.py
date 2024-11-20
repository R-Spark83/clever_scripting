#!/usr/bin/env python
# coding: utf-8

# ## Reporting for Revenues obtained by Ecommerce Sales

# Locating the directory for the file output to the ecommerce report for use as the base for this report and instantiating a dataframe object.

# In[112]:


os.chdir(r'C:\Users\Artificial_Directory\FY25')
df_main = pd.read_csv('Oct_FY25_ecom.csv')
df_table = pd.DataFrame()


# Reading the Country/Purchase Point attribute of the dataframe into a list, then to a set to access unique values for each country.

# In[114]:


lst_main_PP = list(df_main['Purchase Point'])
df_table['Country'] = list(set(lst_main_PP))


# Checking the output to ensure the code executed correctly.

# In[116]:


df_table['Country']


# Setting date-values for the reporting period to also serve as a filter to the report.

# In[117]:


start_date_current = datetime.strptime('2024-10-01','%Y-%m-%d')
end_date_current = datetime.strptime('2024-10-31','%Y-%m-%d')


# Checking the shape of the dataframe

# In[118]:


df_main.shape


# Filtering the dataframe for the records that meet the criteria of being counted in the reporting period.

# In[119]:


df_main[(df_main["Status"] == "Complete") & ((pd.to_datetime(df_main["Completed At"], dayfirst = True) >= start_date_cur) & (pd.to_datetime(df_main["Completed At"], dayfirst = True) <= end_date_cur))]


# Detailing each record in granularity to determine how much was spent according to the channel used to make the purchase in the timeframe that we are observing.

# In[122]:


j = 0
#Instantiating some empty lists to hold the dollar value and count of transactions attributable to each channel.
cash_vals = []
PoD_vals = []
RF_vals = []
SC_vals = []
cash_count_lst = []
PoD_count_lst = []
RF_count_lst = []
SC_count_lst = []
'''For the structure of code we explore to solve this problem we will be employing a for loop in a for loop to match the
criteria of each element in the dataframe to the correct country and payment category for proper record-keeping and aggregation
of transactions'''
lst_country = list(df_table['Country'])
large_country = list(df_main['Purchase Point'])
lst_status = list(df_main['Status'])
lst_PayMeth = list(df_main['Payment Method'])
lst_USD_total = list(df_main['USD Grand Total'])
lst_dates = pd.to_datetime(df_main['Completed At'], dayfirst = True)
x= set(lst_country)
for j in range(len(df_table['Country'])):
    #Instatiating values for transaction values and counts
    '''In the outer loop we will go through the set of countries in the df_table list to extract the unique value to match 
    with the country names in the inner loop'''
    val_sum = 0
    PoD_sum = 0
    RF_sum = 0
    SC_sum = 0
    cash_count = 0
    PoD_count = 0
    RF_count = 0
    SC_count = 0
    '''In the for loop we go through line by line to test whether the loop conditions are met mainly if the transaction is 
    coded as complete, the payment method, within the correct date range and that the country value corresponding to the record
    in the loop matches the country value specified in the outer loop'''
    for i in range(len(df_main['ID'])):
        if((lst_status[i] == "Complete") & ((lst_PayMeth[i] == "Credit/Debit Card")|(lst_PayMeth[i] == "Credit Card/Debit Card")) & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            val_sum += lst_USD_total[i]
            cash_count += 1
        elif((lst_status[i] == "Complete") & (lst_PayMeth[i] == "Pay on Delivery") & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            PoD_sum += lst_USD_total[i]
            PoD_count += 1
        elif((lst_status[i] == "Complete") & (lst_PayMeth[i] == "Ready Finance Application") & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            RF_sum += lst_USD_total[i]
            RF_count += 1
        elif((lst_status[i] == "Complete") & ((lst_PayMeth[i] != "EMMA") | (lst_PayMeth[i] != "Gift Card")) & (lst_country[j] == large_country[i]) & (lst_dates[i] >= start_date_cur) & (lst_dates[i] <= end_date_cur)):
            SC_sum += lst_USD_total[i]
            SC_count += 1
    SC_vals.append(SC_sum)
    cash_vals.append(val_sum)
    PoD_vals.append(PoD_sum)
    RF_vals.append(RF_sum)
    cash_count_lst.append(cash_count)
    PoD_count_lst.append(PoD_count)
    RF_count_lst.append(RF_count)
    SC_count_lst.append(SC_count)
    j+=1


# Here we update the list from the set of country values with the corresponding values to our different payment methods and counts of the transactions which are all added as new dimensions to the dataframe (coming from lists) which serves to complete the requirement of the exercise.

# In[123]:


df_table['Cash Payments'] = cash_vals
df_table['PoD'] = PoD_vals
df_table['RF'] = RF_vals
df_table['Other'] = SC_vals


# In[127]:


df_table['Cash Count'] = cash_count_lst
df_table['POD Count'] = PoD_count_lst
df_table['RF_count'] = RF_count_lst
df_table['SC_count'] = SC_count_lst


# In[128]:


df_table.head(13)


# In[ ]:


df_table.to_csv(r'C:\Users\roy_shaw\Artificial_Directory\FY25\October_FY25_Alt.csv')


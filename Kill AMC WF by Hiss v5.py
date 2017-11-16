# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 08:46:23 2017

@author: bangaloremohan.shiva
"""

FAK_Aanalysis_Data = read_csv("FAK Analysis Data 1002.csv")
Geo_Location = read_csv("Geo Location.xlsx")

Geo_Location['Concatenated'] = Geo_Location[PA Cust MAD cd].map(str) + Geo_Location['PA Cust Agmt Sfx Cust Agrmt Sfx'].map(str) + \
Geo_Location['Sls Rgn Sls Rgn Nm'].mqap(str)

Geo_Location_Unique = Geo_Location.drop_duplicates('Concatenated')

Geo_Location_Select = Geo_location_Unique.loc['PA CUST MAD Code', 'PA Cust NM1', 'PA CUST PSTL CD', 'PA Cust Agrmt SFX Cust AGmt SFx', 'Sls Rgn Sls Rgn Nm']

from datetime import datetime.strptime
import pandas as pd

FAK_Analysis_Data['New_Pickup_date0'] = pd.to_datetime(raw_data['Pickup_date0'], format = '%d%b%Y:%H:%M:%S.%f')

FAK_Analysis_Data['New_Delivery_date'] = pd.to_datetime(raw_data['Delivery_Date'], format = '%d%b%Y:%H:%M:%S.%f')

AMC_Stair_Step_Raw_Data = pd.merge(FAK_Analysis_Data, Geo_Location, how = 'inner', left_on = ['MAD_Code', 'Cust_Agrmt_Sfx'], right_on = ['PA CUST MAD CD', 'PA CUST AGRMT SFX Cust AGRMT SFX'])

AMC_Stair_Step_Raw_Data_Dropped = AMC_Stair_Step_Raw_Data.drop(['Customer ID', 'Pickup_Date0','PA Cust MAD CD', 'PA Cust PStl Cd', ], axis = 1 , inplace = FALSE)
 
AMC_Stair_Step_Dropped['Linehaul Charge'] = AMC_Stair_Step_Raw_Data_Dropped['RtdShpmtLnhlChrgAmt'] - AMC_Stair_Step_Raw_Data_Dropped['RtdShpmtdSCNTAMT']

AMC_Stair_Step_Dropped['Accessorial'] = AMC_Stair_Step_Raw_Data_Dropped['Shpmt_Net_rev'] - AMC_Stair_Step_Raw_Data_Dropped['Linehaul charge']

AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTOBIINCNTV'] = np.where(AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTOBIINCNTV'] = NULL, 0, REVSHPMTOBIINCNTV)

conditions =  [(AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <=500, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 1200, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 2000)]

choices = ['Zone1', 'Zone2', 'Zone3']

AMC_Stair_Step_Raw_Data_Dropped['Zone Flag'] = np.select(conditions, choices, default = 'Zone 4')

AMC_Stair_Step_Raw_Data_Dropped['LAE Flag'] = np.where([NATIONAL_ACCOUNT_IND0] == 'N' and ['CUSTCOL_334'] == 'N', 'LAE', 'Non-LAE')

AMC_Yes_Ind = AMC_Stair_Step_Raw_Data_Dropped[AMC_Stair_Step_Raw_Data_Dropped.AMC_ind == 'Y']
AMC_No_Ind =  AMC_Stair_Step_Raw_Data_Dropped[AMC_Stair_Step_Raw_Data_Dropped.AMC_ind == 'N']

conditions2 =  [(AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <=500, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 1200, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 2000)]

choices2 = [90, 115, 130]

AMC_Yes_Ind['New LineHaul Charge'] = np.select(conditions2, choices2, default = 150)

AMC_Yes_Ind['Incrementatl revenue'] = AMC_Yes_Ind['New Linehaul charge'] - AMC_Yes_Ind['Linehaul charge'] 

AMC_Yes_Ind['Impacted shipment'] = 'AMC'

# Add a select statement here for the AMC_Yes_Ind branch

# AMC_No_Ind Branch

conditions2 =  [(AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <=500, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 1200, AMC_Stair_Step_Raw_Data_Dropped['REVSHPMTLOH'] <= 2000)]

choices2 = [90, 115, 130]

AMC_No_Ind['New LineHaul Charge'] = np.select(conditions2, choices2, default = 150)

AMC_No_Ind['Incrementatl revenue'] = AMC_Yes_Ind['New Linehaul charge'] - AMC_Yes_Ind['Linehaul charge'] 

AMC_No_Ind['Impacted shipment'] = np.where(AMC_No_Ind['Incremental revenue'] > 0, 'New AMC', 'Unimpacted')

AMC_No_Ind['Discount'] = AMC_No_Ind['RTDSHPMTDSCNTAMT'] / RTDSHPMTDSCNTAMT['RTDSHPMTLNHLCHRGAMT']

# Add a select statement here for the AMC_No_Ind branch

AMC_No_Ind = AMC_No_Ind[AMC_No_Ind['RTDSHPMTLNH'] <= 30]

# Union of the 2 sets of data - AMC Yes and AMC No

AMC_Union = concat(AMC_Yes_Ind, AMC_No_Ind)

# Summary 2017 Zone based AMC Output calculations

AMC_Union = AMC_Union['MAD_Code', 'ZOne', 'Incremental_Revenue', 'Shpmt_Net_Rev', 'Pro_Number']

AMC_Groupby1 = AMC_Union.groupby(['MAD_Code']).agg({'Sum_Shpmt_Net_Rev' : 'sum', 'Count_of_Pro' : 'sum'})

AMC_Groupby2 = AMC_Union.groupby(['MAD Code', 'Zone']).agg({'Incremental Revenue' : 'sum', 'Shpmt_Net_Rev' : 'sum','Pro_Number' : 'count'})

AMC_Transpose_Pivot = pd.pivot_table(AMC_Groupby1, index =['MAD_Code'], values = ['Sum Incremental Revenue', 'Sum_Shpmt_Net_Rev', 'Count_Pro_Number'], columns : ['Zone'])

AMC_Transpose_Pivot['Total AMC'] = max(int(Zone_1), 0) + max(int(Zone_2), 0) + max(int(Zone_3), 0) + max(int(Zone_4), 0)

# Final Data Generation

Summary_2017_Zone_Based_AMC = AMC_Groupby2.join(AMC_Transpose_Pivot, on = 'MAD_Code')










   
import pandas as pd
import os

df=pd.read_csv("raw/data_export.csv", parse_dates=["event_date", "purchase_dt"], decimal=",", encoding="iso8859_15", sep=";")
df
path=os.getcwd()+"/output" #Getting the path for output from the current diretory
if os.path.exists(f"{path}") is False: #creating the directory if it does not exist
    os.mkdir(f"{path}")
for x in ["ticket_types", "person_types", "preiscode","channel"]: #Iterating through the columns with relevant unique values(pre determined)
  temp_df=df.groupby(f"{x}").agg({"price":["sum","count"]}) #Aggregating the prices while grouping by unique values
  temp_df.columns = ['_'.join(col) for col in temp_df.columns] #Flattening the multi_index for better redability
  temp_df.to_csv(f"{path}"+"/"+f"{x}"+".csv") #exporting in the respective csvs

event_date_df=df.groupby("event_date").agg({"price":["sum","count"]}).sort_values([('price', 'sum')])
event_date_df.columns = ['_'.join(col) for col in event_date_df.columns]
event_date_df.to_csv(f"{path}"+"/fingerprint.csv")
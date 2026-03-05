import pandas as pd
from import_data import sql_import

year = 2025

def get_data_from_gdata():
    #import sites
    sites = pd.read_csv("data/sites.csv", header=0, skipinitialspace=True, skip_blank_lines=True) # quoting, igonre quotes is 3
    print(sites)
    # create blank df
    raw_data = pd.DataFrame(columns= ["datetime"])
   
    # iterate over  sites, for discharge need to run water_level and discharge as this is a function from cache
    for index, row in sites.iterrows():
        
        #print(row["site"])
        parameter = row["parameter"]
        parameter = parameter.strip()
        site = row["site"]
        #print("parameter", parameter)
        # import data for each site
        df = sql_import(parameter, site, f"{year}-01-01 00:01", f"{year}-12-31 23:59")
       
        #df.rename(columns={'corrected_data': f'water_level'}, inplace=True)
       
        if parameter == "discharge": # use discharge column
            df = df[["datetime", "discharge", "estimate", "warning"]].copy()
            df = df.rename(columns={"discharge": "corrected_data"})
        df["site"] = row["site"]
        df["site_name"] = row["site_name"]
        df["parameter"] = parameter
        #print(df)
        desired_order = ["datetime", "site", "site_name", "parameter", "corrected_data", "estimate", "warning", "non_detect"]  
        existing_columns = [col for col in desired_order if col in df.columns]     
        # Reorder the DataFrame columns
        df = df[existing_columns].copy()
       
        raw_data = pd.concat([raw_data, df], ignore_index = True)

        #raw_data = raw_data.merge(df, on = "datetime", how = "outer")
       
    raw_data.reset_index(drop=True, inplace=True)
    
    raw_data.to_json(f'data/hydrological_data/raw_hydrological_data_{year}.json', orient='records')
    print("saved")
    print(raw_data)
    # save as json
get_data_from_gdata()

# https://stackoverflow.com/questions/12229580/python-importing-a-sub-package-or-sub-module
# See this post for a discussion of how to import these mofos

import requests
import pandas as pd

class rw_api_tools:
    def __init__(self):
        """constructor for this class"""
        self.data = "None yet"
        
    def get_rw_datasets(provider=None):
        url = "https://api.resourcewatch.org/v1/dataset?sort=slug,-provider,userId&status=saved&includes=metadata,vocabulary,widget,layer"
        payload = { "application":"rw", "page[size]": 100000000000}
        res = requests.request("GET", url, params=payload)
        data = res.json()["data"]

        datasets_on_api = {}
        for ix, dset in enumerate(data):
            atts = dset["attributes"]

            metadata = atts["metadata"]
            layers = atts["layer"]
            widgets = atts["widget"]
            tags = atts["vocabulary"]

            datasets_on_api[dset["id"]] = {
                "name": atts["name"], 
                "slug": atts["slug"],
                "provider":atts["provider"],
                "date_updated":atts["updatedAt"],
                "num_layers":len(layers),
                "layers": layers,
                "num_widgets":len(widgets),
                "widgets": widgets,
                "num_metadata":len(metadata),
                "metadata": metadata,
                "num_tags":len(tags),
                "tags":tags,
                "table_name":atts["tableName"],
                "position":ix

            }



        current_datasets_on_api = pd.DataFrame(datasets_on_api).transpose()
        current_datasets_on_api.index.rename("rw_id", inplace=True)
        current_datasets_on_api.sort_values(by=["date_updated"], inplace=True, ascending = False)
           
        if provider == None:
            
            matches = current_datasets_on_api
            print("num datasets: ", matches.shape[0])
            return(matches)
        
        elif (provider in current_datasets_on_api["provider"].unique()):
            
            match_ix = current_datasets_on_api["provider"]==provider
            matches = current_datasets_on_api.loc[match_ix]
            print("num datasets: ", matches.shape[0])
            return(matches)
        
        else:
            
            matches = pd.DataFrame()
            print("Not a valid provider!")
            print("num datasets: ", 0)
            return(matches)
                       

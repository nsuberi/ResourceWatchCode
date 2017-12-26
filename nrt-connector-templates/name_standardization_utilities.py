import numpy as np
from misc_utilities import read_from_S3
import logging

### Standardizing ISO3 codes

CONVERSIONS = "resourcewatch/blog_data/GHG-GDP_Divergence_D3/Conversions/"
wb_name_to_iso3_conversion = read_from_S3(s3_bucket, CONVERSIONS+"World Bank to ISO3 name conversion.csv")
def add_iso(name):
    try:
        return(wb_name_to_iso3_conversion.loc[name,"ISO"])
    except:
        return(np.nan)

# Dropping any countries not desired in the result

drop_patterns = "Arab World, Middle income, Europe & Central Asia (IDA & IBRD countries), IDA total, Latin America & the Caribbean (IDA & IBRD countries), Middle East & North Africa (IDA & IBRD countries), blank (ID 268), Europe & Central Asia (excluding high income), IBRD only, IDA only, Early-demographic dividend, Latin America & the Caribbean (excluding high income), Middle East & North Africa, Middle East & North Africa (excluding high income), Late-demographic dividend, Pacific island small states, Europe & Central Asia, European Union, High income, IDA & IBRD total, IDA blend, Caribbean small states, Central Europe and the Baltics, East Asia & Pacific, East Asia & Pacific (excluding high income), Low & middle income, Lower middle income, Other small states, Latin America & Caribbean, East Asia & Pacific (IDA & IBRD countries), Euro area, OECD members, North America, Middle East & North Africa (excluding high income), Post-demographic dividend, Small states, South Asia, Upper middle income, World, heavily indebted poor countries (HIPC), Least developed countries: UN classification, blank (ID 267), blank (ID 265), Latin America & Caribbean, IDA & IBRD total, IBRD only, Europe & Central Asia, sub-Saharan Africa (excluding high income), Macao SAR China, sub-Saharan Africa, pre-demographic dividend, South Asia (IDA & IBRD), sub-Saharan Africa (IDA & IBRD), Upper middle income, fragile and conflict affected"
drop_patterns = [patt.strip() for patt in drop_patterns.split(",")]

def pick_wanted_entities(entities, drop_patterns=drop_patterns):
    """
    Input:
    * a list of entities that correspond to a dataframe of observations for which these may be in the index
    * a list of which entities you'd like to eliminate

    Output: which indices to keep from the originating dataframe to eliminate the desired entities
    """

    ix_to_keep = [ix for ix, entity in enumerate(entities) if entity not in drop_patterns]
    return(ix_to_keep)

entities = ["France", "Ghana", "Middle income", "Europe & Central Asia (IDA & IBRD countries)", "IDA total"]

logging.info(pick_wanted_entities(entities))
logging.info(pick_wanted_entities(entities, drop_patterns=["France", "Ghana"]))

# 3rd party libraries 

import shutil
import datetime

def cleanUp(tmpDataFolder):
    shutil.rmtree(tmpDataFolder)
    
def create_formatted_dates(ref_time, time_displacements, date_pattern="%Y-%m-%d"):
    """
    Inputs:
    * ref_time in datetime.datetime format
    * list of time values corresponding to data in the nc file
    ** time values are expressed in days since the ref_time
    Outputs:
    * list of strings in desired date_pattern
    """
    formatted_dates = [(ref_time + datetime.timedelta(days=int(time_disp))).strftime(date_pattern) for time_disp in time_displacements]
    return(formatted_dates)
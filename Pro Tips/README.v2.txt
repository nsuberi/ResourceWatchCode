
- Panoply is a great netCDF viewer: http://www.giss.nasa.gov/tools/panoply/
- Near real time GEOS-5 fields are provided on an experimental basis.

------------------------------------------------------------------------------
Data
------------------------------------------------------------------------------

File organization:
- Files are organized by type (wxInput or fwiCalcs), year, and precipitation input type, PRECTYPE (MERRA2, MERRA2.CORRECTED, Sheffield, CPC, GPCP, TRMM, GPM.FINAL, GPM.EARLY)
- /wxInput/MERRA2/YYYY/Wx.MERRA2.Daily.Default.YYYYMMDD.nc: common MERRA2 weather input (T, RH, windspeed, snow cover) 
- /wxInput/PRECTYPE/YYYY/Prec.PRECTYPE.Daily.Default.YYYYMMDD.nc: precipitation input by PRECTYPE
- /fwiCalcs.MERRA2/Default/PRECTYPE/YYYY/FWI.PRECTYPE.Daily.Default.YYYYMMDD.nc: FWI components by PRECTYPE
- 'Default' refers to the default DMC/DC shutdown and startup procedures. The global attributes in each netcdf file mainly refer to the startup procedure free parameter settings.
- in each file, variables are prefixed according to their data source

Input data:
- GEOS-5 near real time analysis fields described here: http://gmao.gsfc.nasa.gov/forecasts/
- MERRA2 is described here: https://gmao.gsfc.nasa.gov/reanalysis/MERRA-2/
- Sheffield precipitation is described here: http://hydrology.princeton.edu/data.pgf.php
- CPC daily precipitation is described here: ftp://ftp.cpc.ncep.noaa.gov/precip/CPC_UNI_PRCP/GAUGE_GLB/DOCU/PRCP_CU_GAUGE_V1.0GLB_0.50deg_README.txt
- GPCP one-degree-daily precipitation is described here: http://precip.gsfc.nasa.gov/gpcp_daily_comb.html
- TRMM daily precipitation is described here: https://pmm.nasa.gov/data-access/downloads/trmm
- GPM/IMERG daily precipitation is described here: https://pmm.nasa.gov/data-access/downloads/gpm
- GlobCover 2009 is described here: http://due.esrin.esa.int/page_globcover.php. This is now used to mask out non-vegetated areas.

Known issues:
- GEOS-5 data are unavailable for Aug 20 and 21, 2014. These have been replaced with data for Aug 19th and 22nd respectively for the purposes of continuing FWI calculations, but will cause artificially persistent weather inputs for those two days.
- 1980 is a startup year and should not be included in any analysis, except to examine the behavior of moisture code startup. Most data will be missing during the first 3 months of 1980.
- CPC precipitation is occasionally missing for single days over all of Eurasia, Southeast Asia, Africa and Canada. This first occurs on 19810102, happens frequently from 1983-1987, and on 19920801, 20040910. All CPC data is missing on 20070226. In these cases, CPC-based calculations shutdown and restart. No attempt is currently made to fix this. Instead, MERRA2.CORRECTED-based calculations are recommended instead during periods affected by missing CPC data.
- Calculations appear to shutdown too fast in cold regions, compared to CWFIS at least. Need to re-examine startup/shutdown thresholds, compare MERRA T & snow estimates to station data.
- The discrete latitudinal bands for the seasonal DMC and DC drying factors create artificial jumps in those values in the tropics and subtropics across latitudes. Smooth day length factor adjustments need to be used.

Please send feedback to Robert Field (robert.field@columbia.edu).

Reference: Field, R.D., A.C. Spessa, N.A. Aziz, A. Camia, A. Cantin, R. Carr, W.J. de Groot, A.J. Dowdy, M.D. Flannigan, K. Manomaiphiboon, F. Pappenberger, V. Tanpipat, X. Wang, Development of a Global Fire Weather Database, Natural Hazards and Earth System Sciences, 15, 1407-1423, doi:10.5194/nhess-15-1407-2015, 2015.

Funding: GFWED development is supported by the NASA Precipitation Measurement Missions Science Team and the NASA Modeling and Analysis Program.

------------------------------------------------------------------------------
Matlab code
------------------------------------------------------------------------------
Production
- MakeAGlobalFWIDataset.m: This is the main code for producing GFWED.
- CalcFWITimeSeriesWithStartup: This is the FWI equation code.
- InitializeParamSets: This primarily sets parameters related to winter shutdown and spring startup. 

Diagnostics
- ProcessRawYearlyISHDataFiles.m: this decodes the raw NCDC ISH hourly wx data and writes out files for FWI input.
- CompareGlobalFWIDatasetToStnCalcs.m: This computes station-based FWI for a representative set of regions and extracts GFWED values for those regions.
- MakeFWIPlots.m: This makes time series plots comparing GFWED and station-based FWI calculations for a representative set of regions.
- PlotGlobalStats.m: This make time series plots of global mean and maximum FWI, and the maximum daily change in FWI. This is to identify any obvious secular changes in the components that could be caused by changes in the input data related to their assimilation procedures, rain gauge coverage etc...

------------------------------------------------------------------------------
History
------------------------------------------------------------------------------
Jan 21 2017:
- Near real time data using GEOS-5 assimilation fields added on an experimental basis.

Nov 7 2016: v2.0 
- FWI calculations have been added based on MERRA2 gauge-corrected precip (1980-present), GPCP (1997-present), TRMM (1998-2014) GPM.FINAL (20140401-present w/ ~6 months latency), GPM.EARLY (20150401-present w/ 1-day latency). 
- Different FWI calculations are now available at the resolution of their precipitation input. 
- Files are now organized according to precipitation source. There are now individual daily files rather than 1-months worth of daily fields.
- Non-vegetated areas are now masked out based on excluding bare areas, permanent snow/ice, and water from ESA GlobCov 2009 land cover, rather than ad-hoc estimates of non-vegetated areas based on mean annual temperature and precipitation.
- Calculations now restart from last available moisture code *.nc file
- Datasets available later than MERRA2 (GPCP,TRMM,GPM) use MERRA2's moisture codes from day before that dataset's start date, if available. For example GPM.FINAL is first available on 20140401, so uses MERRA2 moisture codes from 20140331.
May 18, 2016: v1.6 switched to MERRA2. Using instantaneous fields from MERRA2, rather than time-averaged (over model time-steps within an hour). Instantaneous fields are closer to how the FWI values are calculated operationally.
Apr 5, 2015: v1.5 update through 2014. Changed to original 1x1 deg Sheffield precip (up to 2010) instead of derived dataset from MPI. Station data checked through 2014 against NCDC ISD only for Montane Cordillera, Mato Grosso and Southern Kalimantan.
Nov 8, 2014: v1.4, changed dimension order to time, lat, lon
Aug 2, 2014: v1.3, reduced precip threshold for mask to 0.25 mm/day, more station data
Apr 13, 2014: v1.2, added CPC gauge-count maps, comparison between station FWI and regional boxes around pairs.
Dec 14, 2013: v1.1, added CPC precip based calculations, extended calculations until 2012 (except for Sheffield, which is until 2008)

------------------------------------------------------------------------------
Ideas for future development
------------------------------------------------------------------------------
- add VPD, Nesterov, McArthur, NFDRS, Keetch-Byram, any other indices requiring only the most basic of weather data
- add CAPE/Haines Index
- add NOAA CMORPH v1.0 precipitation estimate: ftp://ftp.cpc.ncep.noaa.gov/precip/CMORPH_V1.0/RAW/0.25deg-DLY_00Z/
	From NCAR in netCDF: https://climatedataguide.ucar.edu/climate-data/cmorph-cpc-morphing-technique-high-resolution-precipitation-60s-60n
- include other MERRA2 land surface parameters relevant to fire: surface soil wetness, LAI, soil moisture at different depths, greenness fraction
- test different startup procedures: no consideration of over winter snow amount, sensitivity to free parameter changes in DC/DMC overwintering and startup, Turner and Lawson a,b approach, Empirical DMC/DC estimates from MERRA2 land surface model.
- smooth day-length drying factors across latitudes for DMC/DC 
- hourly calculations using diurnal FFMC model
- input from other state-of-the-art reanalyses: ERA-interim, ERA-5, CFSR, JRA

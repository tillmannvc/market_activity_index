import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib import patches
import matplotlib.image as mpimg
import numpy as np
import matplotlib.ticker as ticker
from google.cloud import storage
client = storage.Client()

from PIL import Image
from dateutil.relativedelta import relativedelta
from MAI2023.masterFunctions_v20240502 import *

import re
from io import BytesIO
from datetime import datetime
import pandas as pd
import geopandas as gpd

def infoVars(df, mktID, locGroup): # assign info variables based on date and location
    df['mktID'] = mktID
    df['locGroup'] = locGroup
    country = checkLocationFileStatus(mktID, 'country')
    df['country'] = country
    try: # Necessary because some exports have band names starting with 1_ or 2_, not the date. Comes from merge of two image collections ic_old and ic_new
        df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[:8], "%Y%m%d").date()))
    except:
        df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[2:10], "%Y%m%d").date()))
    try:
        df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[9:15], "%H%M%S").time())
    except:
        df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[11:17], "%H%M%S").time())
        
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    #df['time_decimal']=   df['time'].dt.hour + df['time'].dt.minute / 60 + df['time'].dt.second / 3600
    df['time_decimal'] = df['time'].apply(lambda t: t.hour + t.minute / 60 + t.second / 3600)
    df['weekday'] = (df['date'].dt.weekday + 1) % 7
    df['mkt_lat'] = pd.to_numeric(df['mktID'].str.extract(r'lon(-?\d+)_(\d+)').apply(lambda x: f"{x[0]}.{x[1]}", axis=1))
    df['mkt_lon'] = pd.to_numeric(df['mktID'].str.extract(r'lat(-?\d+)_(\d+)').apply(lambda x: f"{x[0]}.{x[1]}", axis=1))
    if country=="Kenya": # For some locations in Kenya, the lon and lat coordinates were flipped in their mktid
        df['origLat'] = df['mkt_lat']
        df.loc[df['mkt_lat'] > 30, 'mkt_lat'] = df['mkt_lon']
        df.loc[df['mkt_lon'] < 30, 'mkt_lon'] = df['origLat']
        df.drop(columns=['origLat'], inplace=True)
    return df

def identifyMktDays(df,minRank, threshold_for_market): # identify market days based on detected areas and their threshold values
    #print('minRank: ', minRank, ', Threshold_for_market: ', threshold_for_market)
    # List all maximum threshold values on the days-of-week where we detected something and that detection falls below a threshold 
    min_thres_by_day = df[df['strictnessRank'] <= threshold_for_market].groupby('weekdayThisAreaIsActive')['strictnessRank'].min()
    #print('strictness rank and active weekdays',min_thres_by_day)
    # Find the clearest detection 
    lowest_thres = min_thres_by_day.min()
    #print('lowest strictness rank',lowest_thres)
    # Filter unique days of week where the threshold is within 3 ranks of the lowest threshold value -> identifies all similarly high detections
    localMktDays = list(min_thres_by_day[min_thres_by_day - lowest_thres <= 3].index.unique())
    #print('localMktDays', localMktDays)
    def find_position(weekday):
        try:
            return list(localMktDays).index(weekday)
        except ValueError:
            return -1  # Return 0 if the weekday is not found in the list
    df['pos'] = df['weekday'].apply(find_position)
    df['mktDay'] = None
    df.loc[(df['weekday'] == df['weekdayThisAreaIsActive']) & (df['pos'] >= 0), 'mktDay'] = 1 # detected market day
    df.loc[(df['weekday'] != df['weekdayThisAreaIsActive']) & (df['pos'] == -1), 'mktDay'] = 0 # detected non-market day
    df.loc[(df['weekday'] != df['weekdayThisAreaIsActive']) & (df['pos'] >= 0), 'mktDay'] = 99 # observation of detected market area for a given weekday on a different weekday
    return df, localMktDays

def cleanActMeasures(df, geos, varsOfInterest): 
    # Set values to NA that exceed the median value per market, weekday of operation
    # and instrument by more than twice the IQR , calculated over the period 
    # outside Covid and for typical times and good images
    #df['time_decimal'] = df['ident'].apply(extract_time_decimal)
    df['diff_to_median_time'] = df.apply(lambda row: abs(row['time_decimal'] - df['time_decimal'].median()), axis=1)
    mask = (
        (df['date'].between('2020-03-01', '2021-02-28')) | # potentially covid-affected
        (df['date'] < '2018-01-01') |                      # generally noisier because of sparse imagery
        (df['diff_to_median_time'] > .5) |                  # differing sun angle
        ((df['clear_percent'].notnull()) & (df['clear_percent'] < 10)) | # noisy imagery
        ((df['cloud_percent'].notnull()) & (df['cloud_percent'] > 50))
    )
    # Create a new column 'exclDates' based on the mask
    df['exclDates'] = mask.astype(int)
    for b in geos: # within each possible area
        df[f'sumsum_maxpMax_{b}'] = df[f'sumsum_maxpMax_{b}'] / df[f'ccount_maxpMax_{b}'] # convert sum variable into mean deviations

        # Typical number of pixels per shape
        max_count = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay'])[f'ccount_maxpMax_{b}'].max().reset_index()
        df = pd.merge(df, max_count, on=[ 'weekdayThisAreaIsActive', 'mktDay'], how='outer', suffixes=('', '_max_count'))        

        for p in varsOfInterest:
            try:
                # set to NA those values coming from images that cover less than 50% of the typical footprint
                df.loc[df[f'ccount_maxpMax_{b}']  < 0.5 *(df[f'ccount_maxpMax_{b}_max_count']), f'{p}_maxpMax_{b}'] = np.nan

                # calculate median, iqr by detected area and sensor, and merge to dataframe
                median = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.5).reset_index()
                df = pd.merge(df, median, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_median'))

                p25 = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.25)
                p75 = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.75)
                iqr = (p75-p25).reset_index()
                df = pd.merge(df, iqr, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_iqr'))
                
                # set to NA those values that are more than twice the IQR above the median
                df.loc[df[f'{p}_maxpMax_{b}']  > (df[f'{p}_maxpMax_{b}_median'] + 2 * df[f'{p}_maxpMax_{b}_iqr']), f'{p}_maxpMax_{b}'] = np.nan
                df = df.drop([f'{p}_maxpMax_{b}_median', f'{p}_maxpMax_{b}_iqr'], axis=1)    

            except Exception as e:
                print('Error in cleanActMeasures', e)
                pass
    return df

def contains_substring(s, substrings):
    for substring in substrings:
        if substring in s:
            return True
    return False

def drop_columns_by_pattern(df, patterns_to_drop):
    for pattern in patterns_to_drop:
        try:
            df = df.drop(df.filter(like=pattern).columns, axis=1)
        except Exception as e:
            print(f"Error occurred while dropping columns for pattern '{pattern}': {e}")
    return df

def determine_sensor(row):
    image_id = row['ident']
    condition1 = '3B' in image_id[-2:]
    condition2 = '_1_' in image_id
    if condition1 or condition2:
        return 'PS2'
    else:
        return 'PSB.SD'
    
def prepare_properties(locGroup, loc, propToDrop):
    df_prop = pd.read_csv(f'gs://exports-mai2023/{locGroup}/properties/propEx_{locGroup}_{loc}.csv')
    
    # Extract 'ident' from 'system:index' column
    df_prop['ident'] = df_prop['system:index'].str.slice(stop=23) 
    # Determine the imagery generation of each image
    df_prop['instrument'] = df_prop.apply(determine_sensor, axis=1)
    # Drop specified properties from the DataFrame
    for prop in propToDrop:
        try:
            df_prop = df_prop.drop(prop, axis=1)
        except KeyError:
            pass
    return df_prop  

def identify_varying_areas(wide_df): # Identify the largest ring in which P75 non-market day activity still does not exceed P50 market day activity
    market_days = wide_df.loc[wide_df['mktDay'] == 1, 'weekday'].unique().tolist()
    gdfs = [] # dataframe to hold the selected shapes
    for market_day in market_days:
        print('market_days', market_days, market_day)

        df_mktDays = wide_df[(wide_df['mktDay'] == 1) 
                     & (wide_df['exclDates'] == 0) 
                     & (wide_df['clear_percent'] > 90) 
                     & (wide_df['weekdayThisAreaIsActive']==market_day) 
                     & (wide_df['weekday']==market_day) 
                     & (wide_df['diff_to_median_time'] <.5)]
        
        filtered_columns_sum = df_mktDays.loc[:, df_mktDays.columns.str.contains('sumsum') & 
                                     ~df_mktDays.columns.str.contains('_100')]

        # Exclude columns that are all NA
        filtered_columns_sum = filtered_columns_sum.loc[:, filtered_columns_sum.notna().any()].columns.tolist()
        
        df_nonmktDays = wide_df[(wide_df['mktDay'] == 0) 
                                & (wide_df['exclDates'] == 0) 
                                & (wide_df['clear_percent'] > 90)
                                & (wide_df['diff_to_median_time'] <.5) 
                                & (wide_df['weekdayThisAreaIsActive']==market_day)]
        p75_nonmktDays_sum = df_nonmktDays[filtered_columns_sum].dropna(subset=filtered_columns_sum, how='all').quantile(0.75)    
    
        # keep high quality images, separately for market and non-market days

        # Calculate variance and mean for percentiles (filtered_columns_p)
        p50_mktDays_sum = df_mktDays[filtered_columns_sum].dropna(subset=filtered_columns_sum, how='all').quantile(0.5)
        result = pd.concat([p50_mktDays_sum, p75_nonmktDays_sum], axis=1)
        result.columns = ['p50_mktDays_sum', 'p75_nonmktDays_sum']

        print(result)
        first_row_index = (result['p75_nonmktDays_sum'] > result['p50_mktDays_sum']).replace(False, np.nan).idxmax()
        if pd.isna(first_row_index):
            first_row_index= result.iloc[-1].name

        print("First row where p75_nonmktDays_sum > p50_mktDays_sum: ",first_row_index)

        # Update DataFrame with name of area per weekday that we consider the market area
        wide_df[f'maxVar_s_{market_day}_maxpMax'] = first_row_index
        #print(loc,first_row_index)
        filtered_gdf = select_areas(market_day, first_row_index)
        gdfs.append(filtered_gdf)

    return wide_df, gdfs

def select_areas(market_day,first_row_index): #select the shapes associated with the selected market area
    # extract substring between second last and last instance of _
    temp = first_row_index.split('_')
    if len(temp) >= 2:
        minRing =  int(temp[-2])
    else:
        minRing = None  # Return None if there aren't enough parts
    print('minRing', minRing)
    # load shapefile    
    shp_path = f'gs://exports-mai2023/{locGroup}/shapes/shp_MpM6_{locGroup}{loc}.shp'
    gdf = gpd.read_file(shp_path)    
    filtered_gdf = gdf[(gdf['weekdayShp'] == market_day) & 
                   (gdf['strictness'] == minRing) & 
                   (gdf['subStrictn'] == 100)].copy()
    #filtered_gdf.plot()
    filtered_gdf.loc[:, 'mktid'] = loc  # Use .loc to set values
    return filtered_gdf

def prepend_zero_if_single_digit(value):
    if len(str(value)) == 1:
        return '0' + str(value)
    else:
        return str(value)

def checkLocationFileStatus(loc, columns):
    #function to check the value of given and tracking column within the master location file
    #cnx:      mySQL connection object
    #locGroup: string (e.g: "79_Tigray_1")
    #loc:      string (e.g: "lon14_115lat38_4743")
    #column:   string (e.g: "00DownStatus")
    #returns:  value in the specified column for the loc 
    
    cnx = None
    cursor = None
    
    if type(columns) == list:
        columnString = ', '.join(columns)
    else:
        columnString = columns
        
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='XX',host='XX',database='mai-database')

            cursor = cnx.cursor()
            query = (f"SELECT {columnString} FROM `mai-database`.location_file WHERE Location = '{loc}'")
            cursor.execute(query)
            response = cursor.fetchall()
            if type(columns) == list:
                return [row for row in response][0]
            else:
                return [row for row in response][0][0]
            
    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")
        
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

        
    
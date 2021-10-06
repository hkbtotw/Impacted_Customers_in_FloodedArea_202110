from h3 import h3
from datetime import datetime, date, timedelta
import pandas as pd
from geopandas import GeoDataFrame
import geopandas as gpd
from shapely.geometry import Point
import pickle
from Credential import *
import glob
import pyproj    #to convert coordinate system
from shapely.geometry import Polygon, mapping




# start_datetime = datetime.now()
# print (start_datetime,'execute')
# todayStr=date.today().strftime('%Y-%m-%d')
# nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
# print("TodayStr's date:", todayStr,' -- ',type(todayStr))
# print("nowStr's date:", nowStr,' -- ',type(nowStr))

def Read_Mobility_IsolationScore_twoWeeks(dateIn):
    print('------------- Start ReadDB : Mobility_IsolationScore_twoWeeks -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_remp


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

           SELECT  [EID]
                    ,[ELat]
                    ,[Elong]
                    ,[Freq]
                    ,[TotalCheckIn]
                    ,[PercentIsolation]
                    ,[MeanCheckIn]
                    ,[ReliabilityWeight]
                    ,[IsolationScore]
                    ,[DBCreatedDateTime]
                    ,[p_name_t]
                    ,[a_name_t]
                    ,[t_name_t]
                    ,[p_name_e]
                    ,[a_name_e]
                    ,[t_name_e]
                FROM [TB_SR_Employee].[dbo].[Mobility_IsolationScore_twoWeeks]
                where DBCreatedDateTime>'"""+str(dateIn)+"""'
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_CVM_CUST_NEW():
    print('------------- Start ReadDB : DIM_LOC_CVM_CUST_NEW -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT  [rowid]
                ,[CUSTM_CODE]
                ,[CUSTM_NAME]
                ,[CUSTM_LAT]
                ,[CUSTM_LNG]
                ,[WORK_SUB_DISTR]
                ,[WORK_DISTR]
                ,[WORK_PROVI]
                ,[WORK_ZIP]
                ,[CUST_CAT]
                ,[CUST_SUB_CAT]
                ,[s_region]
                ,[p_code]
                ,[a_code]
                ,[t_code]
                ,[p_name_t]
                ,[p_name_e]
                ,[a_name_t]
                ,[a_name_e]
                ,[t_name_t]
                ,[t_name_e]
                ,[prov_idn]
                ,[amphoe_idn]
                ,[tambon_idn]
                ,[MIN_PYMT_DATE]
                ,[CUST_CODE_12DIGI]
                ,[IS_VALID_LOC]
                ,[MAX_PYMT_DATE]
            FROM [TSR_ADHOC].[dbo].[DIM_LOC_CVM_CUST_NEW]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_AGSUB():
    print('------------- Start ReadDB : DIM_LOC_AGSUB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT  [REF]
                    ,[CUST_ID]
                    ,[CUST_ID_INT]
                    ,[CUST_NM]
                    ,[LAT]
                    ,[LNG]
                    ,[MIN_YEARMONTH]
                    ,[UPDATED_DATE]
                    ,[p_code]
                    ,[a_code]
                    ,[t_code]
                    ,[p_name_t]
                    ,[p_name_e]
                    ,[a_name_t]
                    ,[a_name_e]
                    ,[t_name_t]
                    ,[t_name_e]
                    ,[s_region]
                    ,[prov_idn]
                    ,[amphoe_idn]
                    ,[tambon_idn]
                    ,[CUST_STS]
                FROM [TSR_ADHOC].[dbo].[DIM_LOC_AGSUB]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Read_DIM_LOC_SSC_CUST():
    print('------------- Start ReadDB : DIM_LOC_SSC_CUST -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT [RowId]
                ,[Customer]
                ,[CustomerDesc]
                ,[CustType]
                ,[CustGrp1]
                ,[CustGrp1Desc]
                ,[ShopType]
                ,[CustGrp2]
                ,[CustGrp2Desc]
                ,[CustGrp3]
                ,[CustGrp3Desc]
                ,[CustGrp4]
                ,[CustGrp4Desc]
                ,[CustGrp5]
                ,[Region]
                ,[LONGITUDE]
                ,[LATITUDE]
                ,[IS_VALID_LATLNG]
                ,[STREET]
                ,[STR_SUPPL3]
                ,[CITY2]
                ,[POST_CODE1]
                ,[CITY1]
                ,[LastUpdate]
                ,[Province_clean]
                ,[Amphur_clean]
                ,[Tambon_clean]
                ,[p_code]
                ,[a_code]
                ,[t_code]
                ,[p_name_t]
                ,[p_name_e]
                ,[a_name_t]
                ,[a_name_e]
                ,[t_name_t]
                ,[t_name_e]
                ,[s_region]
                ,[prov_idn]
                ,[amphoe_idn]
                ,[tambon_idn]
                ,[area_sqm]
                ,[BS_IDX]
                ,[lastactivebillingdate]
            FROM [TSR_ADHOC].[dbo].[DIM_LOC_SSC_CUST]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def Generate_Lat_Lng_ShpFile(dfIn, file_path):
    dfIn.rename(columns={'LAT':'Latitude','LNG':'Longitude'}, inplace=True)
    print(' --> ',dfIn.head(10))
    # 4 Create tuples of geometry by zipping Longitude and latitude columns in your csv file
    geometry = [Point(xy) for xy in zip(dfIn.Longitude, dfIn.Latitude)] 
    #print(' geometry : ',geometry)

    # 5 Define coordinate reference system on which to project your resulting shapefile
    crs = {'init': 'epsg:4326'}

    # 6 Convert pandas object (containing your csv) to geodataframe object using geopandas
    gdf = GeoDataFrame(dfIn, crs = crs, geometry=geometry)
    print(' gdf : ',gdf)
    # 7 Save file to local destination
    output_filename = file_path + "\\data\\" + "DIM_SHAPE.shp"
    gdf.to_file(filename= output_filename, driver='ESRI Shapefile')

    return gdf

def Read_SHAPE_File(file_path,sub_dir,file_name):
    
    # Read file using gpd.read_file()
    #data = gpd.read_file(file_path+"TH_amphure.shp")
    data1 = gpd.read_file(file_path+sub_dir+file_name, encoding = "iso-8859-1")  #ISO-8859-1
    data1 = data1.to_crs(epsg=4326)
    #print(' gpd : ',data['a_name_t'].head(10))
    print(' gpd : ',data1.head(10))

    return data1

def Read_DIM_TH_R_PROVINCE():
    print('------------- Start ReadDB : DIM_TH_R_PROVINCE -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad


    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT [PROVINCE_TH]
                ,[PROVINCE_EN]
                ,[REGION_TBL]
                ,[REGION_SALE]
                ,[REF_PROV]
                ,[COUNTRY]
                ,[BEER_STRATEGIC]
                ,[prov_idn]
                ,[lat]
                ,[lng]
                ,[REGION_THAILAND]
            FROM [TSR_ADHOC].[dbo].[DIM_TH_R_PROVINCE]
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


def MapValue_Value(x,dictIn):
    try:
        mapped=dictIn[x]
    except:
        #print('==> ',x)
        mapped=x
    return mapped
###****************************************************************
# end_datetime = datetime.now()
# print ('---Start---',start_datetime)
# print('---complete---',end_datetime)
# DIFFTIME = end_datetime - start_datetime 
# DIFFTIMEMIN = DIFFTIME.total_seconds()
# print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')
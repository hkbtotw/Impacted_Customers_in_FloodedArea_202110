import geopandas as gpd
import pyproj    #to convert coordinate system
from shapely.geometry import Polygon, mapping
from datetime import datetime, date, timedelta
from Operation_Generation import *
from h3 import h3
import numpy as np
from pandas.core.common import flatten
from tqdm import *
import warnings
import os
from dateutil.relativedelta import relativedelta
from Text_PreProcessing import *
from csv_join_tambon import *

warnings.filterwarnings('ignore')

#enable tqdm with pandas, progress_apply
tqdm.pandas()

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

###########################################################################
previousdayStr=(date.today()-relativedelta(days=1)).strftime('%Y-%m-%d')
firstStr=(date.today()).strftime('%Y%m%d')
print(' previousdayStr : ',previousdayStr)

previousweekStr=(date.today()-relativedelta(days=7)).strftime('%Y-%m-%d')
secondStr=(date.today()-relativedelta(days=6)).strftime('%Y%m%d')
print(' previousweekStr : ',previousweekStr)

# # # 2 Directory containing your .csv files
file_path = os.getcwd()
##file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Geopandas_Sjoin\\'

output1_name='Flood_Report_2021_Detail_'+todayStr+'_2.xlsx'
output2_name='Flood_Report_2021_Summary_'+todayStr+'_2.xlsx'

### input file information 
sub_dir="\\RAW_SHAPE\\"
boundary_dir='\\boundary_data\\'
file_name="flood7days_20211011_20211005.shp"


###  input h3 grid resolution level
h3_level=10

############## Write data
writer = pd.ExcelWriter(output1_name, engine='openpyxl')
writer2 = pd.ExcelWriter(output2_name, engine='openpyxl')

#######################################################################################
def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def Get_h3_to_geo_Lat(hex_id):
    return h3.h3_to_geo(hex_id)[0]
def Get_h3_to_geo_Lng(hex_id):
    return h3.h3_to_geo(hex_id)[1]

def Generate_Boundary_Of_FloodedProvince(sub_dir, file_name):
    # Read file using gpd.read_file()
    #data1 = gpd.read_file(file_path+"TH_amphure.shp")
    # data1 = gpd.read_file(file_path+"flood7days_20211006_20210930.shp")

    # #print(' gpd : ',data['a_name_t'].head(10))
    # print(' gpd : ',data1.head(10))

    data1=Read_SHAPE_File(file_path,sub_dir,file_name)
    print(' data1 : ',data1, '  :: ',type(data1), ' :: ',data1.columns)
    #################  Edit district sub-district province
    dfDummy=pd.DataFrame(data1)
    dfDummy=dfDummy[['gid','tb_tn','ap_tn','pv_tn']].copy()
    print(' dummy shape : ',dfDummy, '  :: ',type(dfDummy), ' :: ',dfDummy.columns)
    dfDummy.to_csv(file_path+'\\'+"file.csv", encoding="latin-1", index=False) 
    del dfDummy
    dfDummy=pd.read_csv(file_path+'\\'+"file.csv", encoding='Thai' )   # 
    dfDummy.columns=['gid','t_name_t','a_name_t','p_name_t']
    print(' dummy1 shape : ',dfDummy, '  :: ',type(dfDummy), ' :: ',dfDummy.columns)

    data1=data1.merge(dfDummy, on="gid", how="left")
    data1.drop(columns=['tb_tn','ap_tn','pv_tn'],inplace=True)
    print(' data1 : ',data1, '  :: ',type(data1), ' :: ',data1.columns)

    del dfDummy

    return data1

def Get_HexId_ProvinceBoundary(boundaryList, h3_level):
    ### fill province's boundary with h3's grid    
    hexList=[]
    for coor in boundaryList:  
        geoJson = {"coordinates": [coor], "type": "Polygon"}   
        #print(' geoJson : ', geoJson)
        hexagons = list(h3.polyfill(geoJson,h3_level))
        #print(' ==> ',len(hexagons))
        hexList.append(hexagons)

    #### Distinct hexagons to find the complete set of hexagons  for MultiPolygon
    totalList=[]
    for n in hexList:
        totalList=totalList+n
        #print(len(n),' ==> ',len(totalList))
    totalList=list(set(totalList))
    #print(' ==> ',len(totalList))
    hexagons=totalList

    # Create dataframe with one columns from hexagons (total hex_id of the selected province) , named it dfHex
    dfHex=pd.DataFrame(hexagons, columns=['hex_id'])
    #dfHex.head(10)
    #print(len(dfHex),' ------  ',dfHex.head(10))
    return dfHex

def Get_Grid_1_Ring_AroundCenter(dfIn):
    hexList=list(dfIn['hex_id'].unique())
    #print(len(hexList), ' ---- hexList --- ',hexList)
    
    allList=[]
    for hex_id in hexList:
        kRingList = h3.k_ring(hex_id, 1)
        #print(' kring : ',kRingList, ' :: ',hex_id, ' ====>', list(set(list([hex_id]+list(kRingList)))))
        allList.extend(list(set(list([hex_id]+list(kRingList)))))
        #print(' loop : ',len(allList))
    del hexList
    #print(len(allList), ' allList : ',allList)
    allList=list(set(allList))
    #print(len(allList), ' allList  2 : ',allList)

    dfDummy=pd.DataFrame(allList, columns=['hex_id'])
    #print(' --- dfDummy : ',dfDummy)

    return dfDummy

def Get_Prov_Region_Dict():
    ## dim province
    dfProv=Read_DIM_TH_R_PROVINCE()

    regionList=list(dfProv['REGION_SALE'].unique())
    region_df=pd.DataFrame(regionList, columns=['Region'])
    print(' region ==>', len(regionList),' :: ',len(region_df),' ==> ',region_df)

    includeList=['PROVINCE_TH',  'REGION_SALE']
    dfRegion=dfProv[includeList].copy()
    print(len(dfRegion),' ---- region ---- ',dfRegion.head(5), ' ---  ',dfRegion.columns)

    ## create dictionary for province region for summary report
    regionDict=dict(dfRegion.values)
    print(' -- region dict : ',regionDict)

    includeList=['PROVINCE_TH',  'REF_PROV']
    dfProv=dfProv[includeList].copy()
    print(len(dfProv),' ---- prov ---- ',dfProv.head(5), ' ---  ',dfProv.columns)

    provinceList=list(dfProv['PROVINCE_TH'].unique())
    province_df=pd.DataFrame(provinceList, columns=['province'])
    print(' province ==>', len(provinceList),' :: ',len(province_df),' ==> ',province_df)

    ## create dictionary for catg mapping , target_tt_hassource => prd_Catg
    provDict=dict(dfProv.values)
    print(' -- prov dict : ',provDict)
    del dfProv, dfRegion
    return regionDict, provDict, province_df, region_df

def Read_CVM(h3_level):
    # ###### check CVM 
    dfCVM=Read_DIM_LOC_CVM_CUST_NEW()
    includeList=[ 'CUSTM_CODE', 'CUSTM_NAME', 'CUSTM_LAT', 'CUSTM_LNG',
        'WORK_SUB_DISTR', 'WORK_DISTR', 'WORK_PROVI', 'WORK_ZIP', 'CUST_CAT',
        'CUST_SUB_CAT', 's_region', 'p_name_t','a_name_t', 't_name_t' ]
    dfCVM=dfCVM[includeList]
    dfCVM=Clean_CustomerId('CUSTM_CODE', dfCVM)
    dfCVM.rename(columns={'CUSTM_LAT':'LAT','CUSTM_LNG':'LNG','s_region':'Region'},inplace=True)
    
    dfCVM['hex_id']=dfCVM.apply(lambda x:GetH3hex(x['LAT'],x['LNG'],h3_level)  ,axis=1)
    print(len(dfCVM),' --- cvm ---- ',dfCVM.head(10), '----- ',dfCVM.columns)
    return dfCVM

def Read_Employee_CheckIn(h3_level):
    ###### check Employee check-in 2 weeks
    dfRemp=Read_Mobility_IsolationScore_twoWeeks(previousweekStr)
    includeList=['EID', 'ELat', 'Elong', 'Freq', 'TotalCheckIn', 'PercentIsolation',
        'MeanCheckIn', 'ReliabilityWeight', 'IsolationScore',
        'DBCreatedDateTime', 'p_name_t', 'a_name_t', 't_name_t', 'p_name_e',
        'a_name_e', 't_name_e']
    dfRemp['Region']=dfRemp.apply(lambda x: MapValue_Value(x['p_name_t'],regionDict)  ,axis=1)
    dfRemp.rename(columns={'ELat':'LAT','Elong':'LNG'},inplace=True)
    dfRemp['hex_id']=dfRemp.apply(lambda x:GetH3hex(x['LAT'],x['LNG'],h3_level)  ,axis=1)
    
    print(len(dfRemp),' --- remp ---- ',dfRemp.head(5), ' :: ',dfRemp.columns)
    return dfRemp

def Read_SSC(h3_level):
    dfSS=Read_DIM_LOC_SSC_CUST()
    includeList=['Customer', 'CustomerDesc', 'CustType', 'CustGrp1',
        'CustGrp1Desc', 'ShopType', 'CustGrp2', 'CustGrp2Desc', 'CustGrp3',
        'CustGrp3Desc', 'CustGrp4', 'CustGrp4Desc', 'CustGrp5',
        'LONGITUDE', 'LATITUDE', 'IS_VALID_LATLNG', 'STREET', 'STR_SUPPL3',
        'CITY2', 'POST_CODE1', 'CITY1',  'Province_clean',
        'Amphur_clean', 'Tambon_clean']
    dfSS=dfSS[includeList].copy()
    dfSS.rename(columns={'Customer':'CUST_ID','LONGITUDE':'LNG', 'LATITUDE':'LAT'},inplace=True)
    dfSS=Clean_CustomerId('CUST_ID', dfSS)
    dfSS['Region']=dfSS.apply(lambda x: MapValue_Value(x['Province_clean'],regionDict)  ,axis=1)
    dfSS=dfSS[dfSS['IS_VALID_LATLNG']==1].copy().reset_index(drop=True)

    #### Compute own p_name_t
    dfSS=Reverse_GeoCoding(dfSS)
    includeList=['CUST_ID', 'CustomerDesc', 'CustType', 'CustGrp1', 'CustGrp1Desc',
        'ShopType', 'CustGrp2', 'CustGrp2Desc', 'CustGrp3', 'CustGrp3Desc',
        'CustGrp4', 'CustGrp4Desc', 'CustGrp5',  'LNG', 'LAT',
            'STREET', 'STR_SUPPL3', 'CITY2', 'POST_CODE1',
        'CITY1', 'Province_clean', 'Amphur_clean', 'Tambon_clean', 'p_name_t', 'p_name_e',
        'a_name_t', 'a_name_e', 't_name_t', 't_name_e', 's_region']
    dfSS=dfSS[includeList].copy()
    dfSS.rename(columns={'s_region':'Region'},inplace=True)
    dfSS['hex_id']=dfSS.apply(lambda x:GetH3hex(x['LAT'],x['LNG'],h3_level)  ,axis=1)   
    
    print(len(dfSS),' RG SS ===== ',dfSS.head(5),' :: ',dfSS.columns)
    return dfSS

def Process_CVM_Information(dfFlood, dfCVM, provinceIn):
    # ##############################################################################################################
    dfFlood=dfFlood.merge(dfCVM, on='hex_id',how='left', indicator=True)

    ### Select only CVM in the area of interest
    dfFlood=dfFlood[(dfFlood['_merge']=='both') & (dfFlood['p_name_t']==provinceIn)].copy().reset_index(drop=True)
    return dfFlood

def Process_REMP_Information(dfFlood, dfRemp, provinceIn):
    # ##############################################################################################################
    dfFlood=dfFlood.merge(dfRemp, on='hex_id',how='left', indicator=True)

    ### Select only CVM in the area of interest
    dfFlood=dfFlood[(dfFlood['_merge']=='both') & (dfFlood['p_name_t']==provinceIn)].copy().reset_index(drop=True)
    return dfFlood

def Process_SSC_Information(dfFlood, dfSS, provinceIn):
    # ##############################################################################################################
    dfFlood=dfFlood.merge(dfSS, on='hex_id',how='left', indicator=True)

    ### Select only CVM in the area of interest
    dfFlood=dfFlood[(dfFlood['_merge']=='both') & (dfFlood['p_name_t']==provinceIn)].copy().reset_index(drop=True)
    return dfFlood

def Count_by_Province(province_df, cvmDf, prvCol):
    
    grouped=cvmDf.groupby(prvCol).size().to_frame().reset_index()
    grouped.rename(columns={prvCol:'province'},inplace=True)
    grouped.columns=['province','Employees_in_FloodedArea_past7days']
    print(' group ---- ', grouped, ' :: ',type(grouped))

    #grouped.to_excel(file_path+'\\'+'Employee_group_'+previousdayStr+'_.xlsx', index=False)
    province_df=province_df.merge(grouped, on="province", how="left")

    return province_df

def Count_by_Region(region_df, cvmDf, regionCol):
    
    grouped=cvmDf.groupby(regionCol).size().to_frame().reset_index()
    grouped.rename(columns={regionCol:'Region'},inplace=True)
    grouped.columns=['Region','Employees_in_FloodedArea_past7days']
    print(' group ---- ', grouped, ' :: ',type(grouped))

    #grouped.to_excel(file_path+'\\'+'Employee_group_'+previousdayStr+'_.xlsx', index=False)
    region_df=region_df.merge(grouped, on="Region", how="left")

    return region_df

########################################################################################
### GEt region and province Dict
regionDict, provDict, province_df, region_df= Get_Prov_Region_Dict()

### Get CVM
dfCVM=Read_CVM(h3_level)

## Get Employee Checkins
dfRemp=Read_Employee_CheckIn(h3_level)

### GEt SSC 
dfSS=Read_SSC(h3_level)


#### Get Flood area from daily GISTDA information
dfFlood=Generate_Boundary_Of_FloodedProvince(sub_dir, file_name)
print(len(dfFlood), ' ---- Flood ---- ',dfFlood.head(10), ' :: ', dfFlood.columns)

# ### Generate Boundary of Flooded Area
# dfHex=pd.DataFrame(Generate_Boundary_Of_FloodedProvince(sub_dir, file_name),columns=['hex_id'])
# print(len(dfHex), ' ---- hex ---- ',dfHex)

floodProvince=list(dfFlood['p_name_t'].unique())
print(len(floodProvince), ' ---- province ------',floodProvince)

###### for testing ###############################
#floodProvince=['สระบุรี']
##################################################

cvmDf=pd.DataFrame()
empDf=pd.DataFrame()
sscDf=pd.DataFrame()

for province in floodProvince:  #[:2]:
    ################# format : file_name='boundary_ชลบุรี.data'
    file_name='boundary_'+province+'.data'
    print(' file_name : ',file_name)

    with open(file_path+'\\boundary_data\\'+file_name,'rb') as filehandle:
        testlist=pickle.load(filehandle)

    #print(' testlist : ',testlist)
    ### Get HexId of provided Hex_h3 level
    dfHex=Get_HexId_ProvinceBoundary(testlist, h3_level)
    dfHex['Center_Lat']=dfHex.apply(lambda x: Get_h3_to_geo_Lat(x['hex_id'])  ,axis=1)
    dfHex['Center_Lng']=dfHex.apply(lambda x: Get_h3_to_geo_Lng(x['hex_id'])  ,axis=1)

    print(len(dfHex), ' ------ province grids ----- ',dfHex.head(10),' :: ',dfHex.columns)

    gpdHex=gpd.GeoDataFrame(dfHex,crs="EPSG:4326", geometry=gpd.points_from_xy(x=dfHex.Center_Lng, y=dfHex.Center_Lat))
    print(len(gpdHex), ' ------ GPD province grids ----- ',gpdHex.head(10),' :: ',gpdHex.columns, ' ==== ',type(gpdHex))
    del dfHex

    ###### Get flood shape of the current province
    df_flood=dfFlood[dfFlood['p_name_t']==province].copy().reset_index(drop=True)
    print(len(df_flood),' ----  flood area --',province, ' ---- ',df_flood.head(3),' :: ',df_flood.columns, ' :: ',type(df_flood))
   
    sjoined_listings = gpd.sjoin(gpdHex, df_flood,op="within")
    print(len(sjoined_listings),' SSC sjoin : ',sjoined_listings.head(5), ' --- ',sjoined_listings.columns)

    del gpdHex, df_flood

    df_sjoin=pd.DataFrame(sjoined_listings).reset_index(drop=True)
    print(len(df_sjoin),' SSC sjoin 2 : ',df_sjoin.head(5), ' --- ',df_sjoin.columns, ' ===  ',type(df_sjoin))
    #df_sjoin.to_csv(file_path+'\\'+'check_effected_area.csv')

    ### find h3's grid hex_id of all effected areas + area around 100m surrounding each effected area
    df_flood=Get_Grid_1_Ring_AroundCenter(df_sjoin)
    df_flood['Center_Lat']=df_flood.apply(lambda x: Get_h3_to_geo_Lat(x['hex_id'])  ,axis=1)
    df_flood['Center_Lng']=df_flood.apply(lambda x: Get_h3_to_geo_Lng(x['hex_id'])  ,axis=1)

    print(len(df_flood), ' ------ FLOOD province grids ----- ',df_flood.head(10),' :: ',df_flood.columns)

    gpdHex=gpd.GeoDataFrame(df_flood,crs="EPSG:4326", geometry=gpd.points_from_xy(x=df_flood.Center_Lng, y=df_flood.Center_Lat))
    print(len(gpdHex), ' ------ GPD province grids ----- ',gpdHex.head(10),' :: ',gpdHex.columns, ' ==== ',type(gpdHex))
    print(len(df_flood),' ---- Flood ----- ',df_flood.head(4),' :: ',df_flood.columns)

    del df_flood
    df_sjoin=pd.DataFrame(gpdHex).reset_index(drop=True)
    print(len(df_sjoin),' SSC sjoin 2 : ',df_sjoin.head(5), ' --- ',df_sjoin.columns, ' ===  ',type(df_sjoin))
    #df_sjoin.to_csv(file_path+'\\'+'check_effected_area_2.csv', index=False)

    #### process CVM customers in impacted areas  Get Summary : Number per Province and REgion and Detail : name and location
    df_flood=Process_CVM_Information(df_sjoin, dfCVM, province)
    print(len(df_flood), ' ---- Flood ---- ',df_flood.head(5),' :: ',df_flood.columns)
    #df_flood.to_excel(file_path+'\\'+'check_flood_cvm.xlsx', index=False)

    cvmDf=cvmDf.append(df_flood).reset_index(drop=True)
    del df_flood
    #### process Employee in impacted areas  Get Summary : Number per Province and REgion and Detail : name and location
    df_flood=Process_REMP_Information(df_sjoin, dfRemp, province)
    print(len(df_flood), ' ---- Flood ---- ',df_flood.head(5),' :: ',df_flood.columns)
    #df_flood.to_excel(file_path+'\\'+'check_flood_emp.xlsx', index=False)

    empDf=empDf.append(df_flood).reset_index(drop=True)
    del df_flood

    #### process SSC in impacted areas  Get Summary : Number per Province and REgion and Detail : name and location
    df_flood=Process_SSC_Information(df_sjoin, dfSS, province)
    print(len(df_flood), ' ---- Flood ---- ',df_flood.head(5),' :: ',df_flood.columns)
    #df_flood.to_excel(file_path+'\\'+'check_flood_emp.xlsx', index=False)

    sscDf=sscDf.append(df_flood).reset_index(drop=True)
    del df_flood







    del gpdHex

##### CVM 
cvmDf.to_excel(writer, sheet_name='CVM',index=False)
empDf.to_excel(writer, sheet_name='EMP',index=False)
sscDf.to_excel(writer, sheet_name='SSC',index=False)

province_df=Count_by_Province(province_df, cvmDf, 'p_name_t')
print(len(province_df),' ---- cvm province --- ',province_df.head(5),' :: ',province_df.columns)
region_df=Count_by_Region(region_df, cvmDf, 'Region')
print(len(region_df),' ---- cvm region --- ',region_df.head(5),' :: ',region_df.columns)

#### Employee
province_df=Count_by_Province(province_df, empDf, 'p_name_t')
print(len(province_df),' ---- employee province --- ',province_df.head(5),' :: ',province_df.columns)
region_df=Count_by_Region(region_df, empDf, 'Region')
print(len(region_df),' ---- employee region --- ',region_df.head(5),' :: ',region_df.columns)

#### SSC
province_df=Count_by_Province(province_df, sscDf, 'p_name_t')
print(len(province_df),' ---- ssc province --- ',province_df.head(5),' :: ',province_df.columns)
region_df=Count_by_Region(region_df, empDf, 'Region')
print(len(region_df),' ---- ssc region --- ',region_df.head(5),' :: ',region_df.columns)





province_df.to_excel(writer2, sheet_name='Province',index=False)
region_df.to_excel(writer2, sheet_name='Region',index=False)
writer.save()
writer2.save()
############### Write data

del dfRemp, dfSS, dfCVM
del province_df, region_df
del dfFlood, df_sjoin, cvmDf, empDf, sscDf
del floodProvince

# ##****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')
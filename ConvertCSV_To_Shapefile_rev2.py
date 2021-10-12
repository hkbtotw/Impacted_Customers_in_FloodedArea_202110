### ref: https://towardsdatascience.com/how-to-easily-join-data-by-location-in-python-spatial-join-197490ff3544
### geopandas  sjoin

# 1 Import Libraries
import pandas as pd
from Operation_Generation import *
from geopandas import GeoDataFrame
from shapely.geometry import Point
from Text_PreProcessing import *
import os
#import fiona
from csv_join_tambon import *

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

output1_name='Flood_Report_2021_Detail_'+todayStr+'_.xlsx'
output2_name='Flood_Report_2021_Summary_'+todayStr+'_.xlsx'

############## Write data
writer = pd.ExcelWriter(output1_name, engine='openpyxl')
writer2 = pd.ExcelWriter(output2_name, engine='openpyxl')

####################################################################
##### Flooded Area Shapefile from GISTDA
### for testing
# sub_dir='\\SHAPE\\'
# file_name='TH_province.shp'
### for operation : raw data with bad encoding
sub_dir='\\RAW_SHAPE\\'
root_word='flood7days_'
#file_name='flood7days_20211007_20211001.shp'
file_name=root_word+firstStr+'_'+secondStr+'.shp'

#### for operation Reference Thai Version from Mac
# sub_dir='\\FLOOD_SHAPE\\'
# file_name='flood7days_20211004_20210928_Ver2.shp'

df_shape=Read_SHAPE_File(file_path,sub_dir,file_name)
print(' shape : ',df_shape, '  :: ',type(df_shape), ' :: ',df_shape.columns)
#################  Edit district sub-district province
dfDummy=pd.DataFrame(df_shape)
dfDummy=dfDummy[['gid','tb_tn','ap_tn','pv_tn']].copy()
print(' dummy shape : ',dfDummy, '  :: ',type(dfDummy), ' :: ',dfDummy.columns)
dfDummy.to_csv(file_path+'\\'+"file.csv", encoding="latin-1", index=False) 
del dfDummy
dfDummy=pd.read_csv(file_path+'\\'+"file.csv", encoding='Thai' )   # 
dfDummy.columns=['gid','t_name_t','a_name_t','p_name_t']
print(' dummy1 shape : ',dfDummy, '  :: ',type(dfDummy), ' :: ',dfDummy.columns)

df_shape=df_shape.merge(dfDummy, on="gid", how="left")
df_shape.drop(columns=['tb_tn','ap_tn','pv_tn'],inplace=True)
print(' shape : ',df_shape, '  :: ',type(df_shape), ' :: ',df_shape.columns)

del dfDummy
###################################################

dfList=list(df_shape['p_name_t'].unique())
listDf=pd.DataFrame(dfList, columns=['province'])
print(len(dfList),' :: ',len(listDf), ' --- list --- ',dfList)
listDf.to_excel(file_path+'\\'+'flooded_province.xlsx')

# #############################################################
# ## dim province
# dfProv=Read_DIM_TH_R_PROVINCE()

# regionList=list(dfProv['REGION_SALE'].unique())
# region_df=pd.DataFrame(regionList, columns=['Region'])
# print(' region ==>', len(regionList),' :: ',len(region_df),' ==> ',region_df)

# includeList=['PROVINCE_TH',  'REGION_SALE']
# dfRegion=dfProv[includeList].copy()
# print(len(dfRegion),' ---- region ---- ',dfRegion.head(5), ' ---  ',dfRegion.columns)

# ## create dictionary for province region for summary report
# regionDict=dict(dfRegion.values)
# print(' -- region dict : ',regionDict)

# includeList=['PROVINCE_TH',  'REF_PROV']
# dfProv=dfProv[includeList].copy()
# print(len(dfProv),' ---- prov ---- ',dfProv.head(5), ' ---  ',dfProv.columns)

# provinceList=list(dfProv['PROVINCE_TH'].unique())
# province_df=pd.DataFrame(provinceList, columns=['province'])
# print(' province ==>', len(provinceList),' :: ',len(province_df),' ==> ',province_df)

# ## create dictionary for catg mapping , target_tt_hassource => prd_Catg
# provDict=dict(dfProv.values)
# print(' -- prov dict : ',provDict)
# del dfProv, dfRegion

# #####################################################################################
# #### Factory
# dfFac=pd.read_csv(file_path+'\\data\\'+'FACTORY.csv')
# print(len(dfFac),' -- Factory -- ',dfFac.head(5),' --- ',dfFac.columns)

# dfFac.rename(columns={'Long':'LNG', 'Lat':'LAT','P name T':'p_name_t','A name T':'a_name_t','T name T':'t_name_t'},inplace=True)
# print(len(dfFac),' ===== ',dfFac.head(5),' :: ',dfFac.columns)



# Fac_gdf=Generate_Lat_Lng_ShpFile(dfFac, file_path)
# print(' Fac gdf : ',Fac_gdf, ':: ',type(Fac_gdf))

# del dfFac

# sjoined_listings = gpd.sjoin(Fac_gdf, df_shape, op="within")
# print(' FAC sjoin : ',sjoined_listings.head(5), ' --- ', sjoined_listings.columns)
# del Fac_gdf
# if(len(sjoined_listings)>0):       
#        df_sjoin=pd.DataFrame(sjoined_listings)
#        df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)

#        df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
#        print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
#        #df_sjoin.to_excel(file_path+'\\'+'FACTORY_intersection.xlsx')
#        ###w write data
#        df_sjoin.to_excel(writer, sheet_name='FACTORY',index=False)

#        grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
#        grouped.columns=['province','FACTORY_in_FloodedArea_past7days']
#        print(' group ---- ', grouped, ' :: ',type(grouped))
#        #grouped.to_excel(file_path+'\\'+'FACTORY_group.xlsx', index=False)  
#        province_df=province_df.merge(grouped, on="province", how="left")

#        grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
#        grouped.columns=['Region','FACTORY_in_FloodedArea_past7days']
#        print(' group ---- ', grouped, ' :: ',type(grouped))
#        region_df=region_df.merge(grouped, on="Region", how="left")
       
# else:
#        print('**************************************************')
#        print('********  No FACTORY in flooded areas ************')
#        print('**************************************************')
#        grouped=pd.DataFrame(list(zip(['-'],['-'])), columns=['province','FACTORY_in_FloodedArea_past7days'])            
#        province_df=province_df.merge(grouped, on="province", how="left")
#        grouped=pd.DataFrame(list(zip(['-'],['-'])), columns=['Region','FACTORY_in_FloodedArea_past7days'])            
#        region_df=region_df.merge(grouped, on="Region", how="left")
#        ###w write data
#        df_sjoin=pd.DataFrame()
#        df_sjoin.to_excel(writer, sheet_name='FACTORY',index=False)




# ######################################################################################
# ####  TBL
# dfTBL=pd.read_csv(file_path+'\\data\\'+'TBL_Location.csv')
# print(len(dfTBL),' -- TBL -- ',dfTBL.head(5),' --- ',dfTBL.columns)

# dfTBL.rename(columns={'Longitude':'LNG', 'Latitude':'LAT'},inplace=True)
# print(len(dfTBL),' ===== ',dfTBL.head(5),' :: ',dfTBL.columns)

# #### Compute own p_name_t
# dfTBL=Reverse_GeoCoding(dfTBL)
# includeList=['Name', 'Sub District', 'District', 'Province', 'LAT', 'LNG',
#        'TBL_LOC_TYP', 'p_name_t', 'p_name_e', 'a_name_t', 'a_name_e', 't_name_t',
#        't_name_e', 's_region']
# dfTBL=dfTBL[includeList].copy()
# dfTBL.rename(columns={'s_region':'Region'},inplace=True)
# print(len(dfTBL),' ===== ',dfTBL.head(5),' :: ',dfTBL.columns)

# TBL_gdf=Generate_Lat_Lng_ShpFile(dfTBL, file_path)
# print(' TBL gdf : ',TBL_gdf, ':: ',type(TBL_gdf))

# del dfTBL

# sjoined_listings = gpd.sjoin(TBL_gdf, df_shape, op="within")
# print(' sjoin : ',sjoined_listings.head(5), ' --- ')
# del TBL_gdf
# if(len(sjoined_listings)>0):       
#        df_sjoin=pd.DataFrame(sjoined_listings)
#        df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)

#        df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
#        print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
#        #df_sjoin.to_excel(file_path+'\\'+'TBL_intersection.xlsx')
#        ###w write data
#        df_sjoin.to_excel(writer, sheet_name='TBL_WH_TRC',index=False)

#        grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
#        grouped.columns=['province','TBLs_in_FloodedArea_past7days']
#        print(' group ---- ', grouped, ' :: ',type(grouped))
#        #grouped.to_excel(file_path+'\\'+'TBL_group.xlsx', index=False)  
#        province_df=province_df.merge(grouped, on="province", how="left")
       
#        grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
#        grouped.columns=['Region','TBLs_in_FloodedArea_past7days']
#        print(' group ---- ', grouped, ' :: ',type(grouped))
#        region_df=region_df.merge(grouped, on="Region", how="left")
       
# else:
#        print('**************************************************')
#        print('********  No TBLs in flooded areas ***************')
#        print('**************************************************')
          
#        grouped=pd.DataFrame(list(zip(['-'],['-'])), columns=['province','TBLs_in_FloodedArea_past7days'])            
#        province_df=province_df.merge(grouped, on="province", how="left")
#        grouped=pd.DataFrame(list(zip(['-'],['-'])), columns=['Region','TBLs_in_FloodedArea_past7days'])            
#        region_df=region_df.merge(grouped, on="Region", how="left")
#        ###w write data
#        df_sjoin=pd.DataFrame()
#        df_sjoin.to_excel(writer, sheet_name='TBL_WH_TRC',index=False)

# #####################################################################################
# ### SSC
# dfSS=Read_DIM_LOC_SSC_CUST()
# includeList=['Customer', 'CustomerDesc', 'CustType', 'CustGrp1',
#        'CustGrp1Desc', 'ShopType', 'CustGrp2', 'CustGrp2Desc', 'CustGrp3',
#        'CustGrp3Desc', 'CustGrp4', 'CustGrp4Desc', 'CustGrp5',
#        'LONGITUDE', 'LATITUDE', 'IS_VALID_LATLNG', 'STREET', 'STR_SUPPL3',
#        'CITY2', 'POST_CODE1', 'CITY1',  'Province_clean',
#        'Amphur_clean', 'Tambon_clean']
# dfSS=dfSS[includeList].copy()
# dfSS.rename(columns={'Customer':'CUST_ID','LONGITUDE':'LNG', 'LATITUDE':'LAT'},inplace=True)
# dfSS=Clean_CustomerId('CUST_ID', dfSS)
# dfSS['Region']=dfSS.apply(lambda x: MapValue_Value(x['Province_clean'],regionDict)  ,axis=1)
# dfSS=dfSS[dfSS['IS_VALID_LATLNG']==1].copy().reset_index(drop=True)

# #### Compute own p_name_t
# dfSS=Reverse_GeoCoding(dfSS)
# includeList=['CUST_ID', 'CustomerDesc', 'CustType', 'CustGrp1', 'CustGrp1Desc',
#        'ShopType', 'CustGrp2', 'CustGrp2Desc', 'CustGrp3', 'CustGrp3Desc',
#        'CustGrp4', 'CustGrp4Desc', 'CustGrp5',  'LNG', 'LAT',
#         'STREET', 'STR_SUPPL3', 'CITY2', 'POST_CODE1',
#        'CITY1', 'Province_clean', 'Amphur_clean', 'Tambon_clean', 'p_name_t', 'p_name_e',
#        'a_name_t', 'a_name_e', 't_name_t', 't_name_e', 's_region']
# dfSS=dfSS[includeList].copy()
# dfSS.rename(columns={'s_region':'Region'},inplace=True)
# print(len(dfSS),' RG SS ===== ',dfSS.head(5),' :: ',dfSS.columns)

# SS_gdf=Generate_Lat_Lng_ShpFile(dfSS, file_path)
# print(' SS gdf : ',SS_gdf, ':: ',type(SS_gdf))

# del dfSS

# sjoined_listings = gpd.sjoin(SS_gdf, df_shape, op="within")
# print(' SSC sjoin : ',sjoined_listings.head(5), ' --- ',sjoined_listings.columns)

# del SS_gdf

# df_sjoin=pd.DataFrame(sjoined_listings)
# print(' SSC sjoin 2 : ',sjoined_listings.head(5), ' --- ',sjoined_listings.columns)

# df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)

# df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
# print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
# #df_sjoin.to_excel(file_path+'\\'+'SS_intersection.xlsx')
# ###w write data
# df_sjoin.to_excel(writer, sheet_name='SS',index=False)

# grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
# grouped.columns=['province','SSs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# #grouped.to_excel(file_path+'\\'+'SS_group.xlsx', index=False)
# province_df=province_df.merge(grouped, on="province", how="left")

# grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
# grouped.columns=['Region','SSs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# region_df=region_df.merge(grouped, on="Region", how="left")

# # ####################################################################################
# ##### AGSUB
# dfAG_SUB=Read_DIM_LOC_AGSUB()
# includeList=['CUST_ID',  'CUST_NM', 'LAT', 'LNG',     
#        'p_name_t', 'p_name_e', 'a_name_t', 'a_name_e', 't_name_t', 't_name_e',
#        's_region','CUST_STS']
       
# dfAG_SUB=dfAG_SUB[includeList].copy()
# dfAG_SUB=Clean_CustomerId('CUST_ID', dfAG_SUB)
# dfAG_SUB.rename(columns={'s_region':'Region'},inplace=True)
# print(len(dfAG_SUB),' ===== ',dfAG_SUB.head(5),' :: ',dfAG_SUB.columns)


# AGSUB_gdf=Generate_Lat_Lng_ShpFile(dfAG_SUB, file_path)
# print(' AGSUB gdf : ',AGSUB_gdf, ':: ',type(AGSUB_gdf))

# del dfAG_SUB

# sjoined_listings = gpd.sjoin(AGSUB_gdf, df_shape, op="within")
# print(' sjoin : ',sjoined_listings.head(5), ' --- ')

# del AGSUB_gdf

# df_sjoin=pd.DataFrame(sjoined_listings)
# df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)

# df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
# print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
# #df_sjoin.to_excel(file_path+'\\'+'AGSUB_intersection.xlsx')
# ###w write data
# df_sjoin.to_excel(writer, sheet_name='AG_SUB',index=False)

# grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
# grouped.columns=['province','AGSUBs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# #grouped.to_excel(file_path+'\\'+'AGSUB_group.xlsx', index=False)
# province_df=province_df.merge(grouped, on="province", how="left")

# grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
# grouped.columns=['Region','AGSUBs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# region_df=region_df.merge(grouped, on="Region", how="left")


# # ##############################################################################################################
# # ###### check CVM 
# dfCVM=Read_DIM_LOC_CVM_CUST_NEW()
# includeList=[ 'CUSTM_CODE', 'CUSTM_NAME', 'CUSTM_LAT', 'CUSTM_LNG',
#        'WORK_SUB_DISTR', 'WORK_DISTR', 'WORK_PROVI', 'WORK_ZIP', 'CUST_CAT',
#        'CUST_SUB_CAT', 's_region', 'p_name_t','a_name_t', 't_name_t' ]
# dfCVM=dfCVM[includeList]
# dfCVM=Clean_CustomerId('CUSTM_CODE', dfCVM)
# dfCVM.rename(columns={'CUSTM_LAT':'LAT','CUSTM_LNG':'LNG','s_region':'Region'},inplace=True)
# print(len(dfCVM),' --- cvm ---- ',dfCVM.head(10), '----- ',dfCVM.columns)

# CVM_gdf=Generate_Lat_Lng_ShpFile(dfCVM, file_path)
# print(' CVM gdf : ',CVM_gdf, ':: ',type(CVM_gdf))

# del dfCVM

# sjoined_listings = gpd.sjoin(CVM_gdf, df_shape, op="within")
# print(' CVM sjoin : ',sjoined_listings.head(5), ' --- ')

# del CVM_gdf

# df_sjoin=pd.DataFrame(sjoined_listings)
# df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)
# df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
# print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
# #df_sjoin.to_excel(file_path+'\\'+'CVM_intersection.xlsx')
# ###w write data
# df_sjoin.to_excel(writer, sheet_name='CVM',index=False)

# grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
# grouped.columns=['province','CVMs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))

# #grouped.to_excel(file_path+'\\'+'CVM_group.xlsx', index=False)
# province_df=province_df.merge(grouped, on="province", how="left")

# grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
# grouped.columns=['Region','CVMs_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# region_df=region_df.merge(grouped, on="Region", how="left")


# # ##############################################################################################################
# ###### check Employee check-in 2 weeks
# dfRemp=Read_Mobility_IsolationScore_twoWeeks(previousweekStr)
# includeList=['EID', 'ELat', 'Elong', 'Freq', 'TotalCheckIn', 'PercentIsolation',
#        'MeanCheckIn', 'ReliabilityWeight', 'IsolationScore',
#        'DBCreatedDateTime', 'p_name_t', 'a_name_t', 't_name_t', 'p_name_e',
#        'a_name_e', 't_name_e']
# dfRemp['Region']=dfRemp.apply(lambda x: MapValue_Value(x['p_name_t'],regionDict)  ,axis=1)
# dfRemp.rename(columns={'ELat':'LAT','Elong':'LNG'},inplace=True)
# print(len(dfRemp),' --- remp ---- ',dfRemp.head(5), ' :: ',dfRemp.columns)


# Remp_gdf=Generate_Lat_Lng_ShpFile(dfRemp, file_path)
# print(' Remp gdf : ',Remp_gdf, ':: ',type(Remp_gdf))

# del dfRemp

# sjoined_listings = gpd.sjoin(Remp_gdf, df_shape, op="within")
# print(' Remp sjoin : ',sjoined_listings.head(5), ' --- ')

# del Remp_gdf

# df_sjoin=pd.DataFrame(sjoined_listings)
# df_sjoin.rename(columns={'p_name_t_right':'province'}, inplace=True)
# df_sjoin['REF_PROV_EN']=df_sjoin.apply(lambda x: MapValue_Value(x['province'],provDict),axis=1)
# print(len(df_sjoin),' ------ ',df_sjoin.head(5),' ----- ',df_sjoin.columns)
# #df_sjoin.to_excel(file_path+'\\'+'Employee_intersection_'+previousdayStr+'_.xlsx')
# ###w write data
# df_sjoin.to_excel(writer, sheet_name='Employee',index=False)

# grouped=df_sjoin.groupby("province").size().to_frame().reset_index()
# grouped.columns=['province','Employees_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))

# #grouped.to_excel(file_path+'\\'+'Employee_group_'+previousdayStr+'_.xlsx', index=False)
# province_df=province_df.merge(grouped, on="province", how="left")

# grouped=df_sjoin.groupby("Region").size().to_frame().reset_index()
# grouped.columns=['Region','Employees_in_FloodedArea_past7days']
# print(' group ---- ', grouped, ' :: ',type(grouped))
# region_df=region_df.merge(grouped, on="Region", how="left")

# province_df.to_excel(writer2, sheet_name='Province',index=False)

# #### drop ALL row
# region_df=region_df.set_index(['Region'])
# region_df = region_df.drop('ALL').reset_index()
# region_df.to_excel(writer2, sheet_name='Region',index=False)

# del  sjoined_listings, df_sjoin , df_shape
# del province_df, region_df

# writer.save()
# writer2.save()
# ############### Write data

# ##****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')
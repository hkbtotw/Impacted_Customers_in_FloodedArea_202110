from typing import no_type_check_decorator
import pandas as pd
from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta
import math
import numpy as np

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))


def Clean_Prefix(x):
    try:
        words = ["นาย","นางสาว","น.","นส.","น.ส."]
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def RemoveWhiteSpace_ALL(x):
    return x.strip().replace(' ','')
def ModifyPhoneNumber(x):
    dummy=x.strip().replace(' ','')
    dummy=dummy.replace('-','')
    print(x, ' ---  ',dummy)
    if(len(dummy)<=10):
        sum=10-len(dummy)
        print(len(dummy),' : ',sum)
        for n in range(sum):
            dummy='0'+dummy
        print(' result : ',dummy)
    return dummy
def Clean_TextGender(x):
    try:
        word_list = ['f.', 'f','หญิง','ญ.','ญ','female']  
        # initializing replace word 
        repl_wrd = 'female'  
        # Replace multiple words with K
        # Using join() + split() + list comprehension
        res = ' '.join([repl_wrd if idx in word_list else idx for idx in x.lower().split()])

        word_list = ['m.', 'm','ชาย','ช.','ช','male']  
        # initializing replace word 
        repl_wrd = 'male'  
        # Replace multiple words with K
        # Using join() + split() + list comprehension
        res = ' '.join([repl_wrd if idx in word_list else idx for idx in res.lower().split()])
    except:
        res=''
    return res
def Clean_TextProvince_Th(x):    
    try:
        words = [r'จังหวัด',r'จ.',r'จ ',r'เมือง']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def Clean_TextDistrict_Th(x):    
    try:
        words = [r'อำเภอ',r'อ.',r'อ ',r'เมือง']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def Clean_TextSubDistrict_Th(x):    
    try:
        words = [r'ตำบล',r'ต.',r'ต ',r'เมือง']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def Clean_TextProvince_En(x):    
    try:
        words = [r'จังหวัด',r'จ.',r'จ ',r'เมือง','Muang','Maung']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def Clean_TextDistrict_En(x):    
    try:
        words = [r'อำเภอ',r'อ.',r'อ ',r'เมือง','Amphor', 'Amphur', 'A.','A ']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def Clean_TextSubDistrict_En(x):    
    try:
        words = [r'ตำบล',r'ต.',r'ต ',r'เมือง','Tambon','Tumbon','T.','T ']  
        themessage = str(x)    
        for word in words:
            themessage =  themessage.replace(word, "").strip()  
    except:
        themessage=x
    return themessage
def ModifyCustomerId(x):
    dummy=str(x).strip().replace(' ','')
    dummy=dummy.replace('-','')
    #print(x, ' ---  ',dummy)
    if(len(dummy)<=12):
        sum=12-len(dummy)
        #print(len(dummy),' : ',sum)
        for n in range(sum):
            dummy='0'+dummy
        #print(' result : ',dummy)
    return dummy

#---------------------- Use function below if the input comes in Dataframe format  ------------------------
def Clean_FirstName(first_name_col, dfIn):  
    dfIn[first_name_col]=dfIn.apply(lambda x:Clean_Prefix(x[first_name_col]),axis=1)
    return dfIn
def Clean_IdCard(idcard_col, dfIn):
    dfIn[idcard_col]=dfIn.apply(lambda x:RemoveWhiteSpace_ALL(x[idcard_col]),axis=1)
    return dfIn
def Clean_MobileNumber(mobile_col, dfIn):
    dfIn[mobile_col]=dfIn.apply(lambda x:ModifyPhoneNumber(x[mobile_col]),axis=1)    
    return dfIn
def Clean_Gender(gender_col, dfIn):
    dfIn[gender_col]=dfIn.apply(lambda x:Clean_TextGender(x[gender_col]),axis=1)    
    return dfIn
def Clean_CustomerId(mobile_col, dfIn):
    dfIn[mobile_col]=dfIn.apply(lambda x:ModifyCustomerId(x[mobile_col]),axis=1)    
    return dfIn

def Clean_Province_Th(province_col,dfIn):
    dfIn[province_col]=dfIn.apply(lambda x:Clean_TextProvince_Th(x[province_col]),axis=1)    
    return dfIn
def Clean_District_Th(district_col,dfIn):
    dfIn[district_col]=dfIn.apply(lambda x:Clean_TextDistrict_Th(x[district_col]),axis=1)    
    return dfIn
def Clean_SubDistrict_Th(subdistrict_col,dfIn):
    dfIn[subdistrict_col]=dfIn.apply(lambda x:Clean_TextSubDistrict_Th(x[subdistrict_col]),axis=1)    
    return dfIn
def Clean_Province_En(province_col,dfIn):
    dfIn[province_col]=dfIn.apply(lambda x:Clean_TextProvince_En(x[province_col]),axis=1)    
    return dfIn
def Clean_District_En(district_col,dfIn):
    dfIn[district_col]=dfIn.apply(lambda x:Clean_TextDistrict_En(x[district_col]),axis=1)    
    return dfIn
def Clean_SubDistrict_En(subdistrict_col,dfIn):
    dfIn[subdistrict_col]=dfIn.apply(lambda x:Clean_TextSubDistrict_En(x[subdistrict_col]),axis=1)    
    return dfIn


# file_path='C:\\Users\\70018928\\Documents\\Project2021\\Ad-Hoc\\CleanText\\'

# file_name_1='DataVaccineSV_20210427_0830_.xlsx'
# file_name_2='DataVaccineSV_20210427_1500.xlsx'
# file_name_3='TemplateVaccineSV_20210426_1930.xlsx'

# cvt={'ALL_EMPID':str,'contact_emdid':str, 'contact_mobile':str}
# dfIn=pd.read_excel(file_path+file_name_1,sheet_name='MST_Employee', converters=cvt )

# print(dfIn.head(10))
# dfDummy=dfIn.head(10).copy()

# first_name_col='Province_1'
# #dfDummy=Clean_FirstName(first_name_col,dfDummy)
# #dfDummy=Clean_IdCard(first_name_col,dfDummy)
# #dfDummy=Clean_Gender(first_name_col,dfDummy)

# dfDummy=Clean_Province(first_name_col,dfDummy)

# dfDummy.to_csv(file_path+'check_prefix.csv')
# print(' ===> ',dfDummy)

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

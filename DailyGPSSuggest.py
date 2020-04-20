#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import re
import datetime
import time
import os
import ntpath
from math import radians, cos, sin, asin, sqrt, atan2
#import geopy
#from geopy import distance

freight_users=['ttuntiwi','ajintana','jtraisor','naridbua','asrangsr','prodwini','nauetavo','InstallWin7','InstallWin10','tleelaha','pkornbon' ,'csajjanu','gjuntnap']
freight_columns=['IO Log region group','Existing Order Alloc Cost','Estimated Frieght Cost'
                 ,'SuggestFreightCost','FinalFreightCost','billing_date','netweight_qty','swap_file','swap_cost'
                ,'basefreight_otm','accessorial_otm','labor_otm','cost_detail' ]
freight_columns2=['ORDER_RELEASE_LINE','BaseFreight_TripFreight','accessorial','labor','BahtPerTon','SOURCE_DATA']

def store_var(store_name=''):
    x=None
    if store_name.lower() in 'search_master':
        x={'table':'','doc_no':'','doc_name':'','DateFrom1':'','DateTo1':''}
    elif store_name.lower() in 'validate_shipment_cost':
        x={'LogConfirmPOD_DateFrom':'' ,'LogConfirmPOD_DateTo':''}
    elif store_name.lower()[:16] in 'search_shipment_report':
        x={'WO_DateFrom':'','WO_DateTo':''
             ,'PreDOCreate_DateFrom':''   ,'PreDOCreate_DateTo':''
             ,'ShipmentCreate_DateFrom':'','ShipmentCreate_DateTo':''
             ,'RequestTo_DateFrom':''     ,'RequestTo_DateTo':''
             ,'LogConfirmTo_DateFrom':''  ,'LogConfirmTo_DateTo':''
             ,'Assign_DateFrom':''        ,'Assign_DateTo':''
             ,'ArriveAtCust_DateFrom':''  ,'ArriveAtCust_DateTo':''
             ,'LogConfirmPOD_DateFrom':'' ,'LogConfirmPOD_DateT':''
             ,'ShipmentID':'','PreDONo':'' ,'DONo':'' ,'PONo':''
             ,'Perspective':'' ,'DFM':'' ,'MultiStopMark':''
             ,'OrderType':''        ,'ShipmentStatus':''
             ,'ShippingCondition':'','ShippingType':''
             ,'LogRegionGroup':''   ,'LogRegionCode':'','IOLogRegionGroup':''
             ,'TZoneDescription':'' ,'ProvinceDescription':''
             ,'BHIndex':''
             ,'SoldToCode':''       ,'SoldToName':''
             ,'ShipToCode':''       ,'ShipToName':''
             ,'LogMatGroup':''
             ,'Plant':'' ,'ShippingPoint':''
             ,'Material':''         ,'MatDescription':''
             ,'TruckID':''          ,'Driver':''
             ,'TransporterCode':''  ,'TransporterName':''
             ,'ConditionGroup5':''  ,'AssignUser':''
             ,'Division':''         ,'SalesOrg':''
             ,'mark_code':''
             ,'ilm_DateFrom':''     ,'ilm_DateTo':''
             ,'gps_result':''       ,'ontime':''
             ,'Is_byShipment':''
             ,'GroupBy_Sum':'' ,'GroupBy_Rows':'' ,'GroupBy_Columns':''}

    elif store_name.lower() in 'search_orders_visibility':
        x = {'option':'','WO_DateFrom':'','WO_DateTo':''}

    else: print('No variable to execute')
    return x

def store_query(store_name='',sql_var=None,is_print_query=True): #if no paremeter, set to None
    x=''
    y='SCCC Data Warehouse'
    if store_name.lower()=='search_master':
        x='EXEC dbo.'+store_name+' \n'
        x+="@table=?        ,@doc_no=?       ,@doc_name=? \n"
        x+=",@DateFrom1=?   ,@DateTo1=? \n"
    elif store_name.lower()=='validate_shipment_cost':
        x='{call dbo.'+store_name+'(@LogConfirmTo_DateFrom=?,@LogConfirmTo_DateTo=?)}' #ODBC format cannot get output param
    elif store_name.lower()=='search_shipment_report':
        x="DECLARE @out nvarchar(max); \n"
        #="{call dbo.SEARCH(@var1=?,@var2=?)}" #ODBC format cannot get output parameters
        x+='EXEC dbo.'+store_name+' \n'
        x+="@WO_DateFrom=?             ,@WO_DateTo=? \n"
        x+=",@PreDOCreate_DateFrom=?   ,@PreDOCreate_DateTo=? \n"
        x+=",@ShipmentCreate_DateFrom=?,@ShipmentCreate_DateTo=? \n"
        x+=",@RequestTo_DateFrom=?     ,@RequestTo_DateTo=? \n"
        x+=",@LogConfirmTo_DateFrom=?  ,@LogConfirmTo_DateTo=? \n"
        x+=",@Assign_DateFrom=?        ,@Assign_DateTo=? \n"
        x+=",@ArriveAtCust_DateFrom=?  ,@ArriveAtCust_DateTo=? \n"
        x+=",@LogConfirmPOD_DateFrom=? ,@LogConfirmPOD_DateTo=? \n"
        x+=",@ShipmentID=? ,@PreDONo=? ,@DONo=? ,@PONo=? \n"
        x+=",@Perspective=? ,@DFM=? ,@MultiStopMark=? \n"
        x+=",@OrderType=?              ,@ShipmentStatus=? \n"
        x+=",@ShippingCondition=?      ,@ShippingType=? \n"
        x+=",@LogRegionGroup=?         ,@LogRegionCode=?      ,@IOLogRegionGroup=? \n"
        x+=",@TZoneDescription=?       ,@ProvinceDescription=? \n"
        x+=",@BHIndex=? \n"
        x+=",@SoldToCode=?             ,@SoldToName=?\n"
        x+=",@ShipToCode=?             ,@ShipToName=? \n"
        x+=",@LogMatGroup=? \n"
        x+=",@Plant=?                  ,@ShippingPoint=? \n"
        x+=",@Material=?               ,@MatDescription=? \n"
        x+=",@TruckID=?                ,@Driver=? \n"
        x+=",@TransporterCode=?        ,@TransporterName=? \n"
        x+=",@ConditionGroup5=?        ,@AssignUser=? \n"
        x+=",@Division=?               ,@SalesOrg=? \n"
        x+=",@mark_code=?"
        x+=",@ilm_DateFrom=?           ,@ilm_DateTo=? \n"
        x+=",@gps_result=?             ,@ontime=? \n"

        x+=",@Is_byShipment=? \n"
        x+=",@GroupBy_Sum=? ,@GroupBy_Rows=? ,@GroupBy_Columns=? \n"

        x+=",@param_out=@out OUTPUT; \n"            "SELECT @out AS the_output;\n"
    elif store_name.lower()=='calculate_missingdo':
        x="EXEC dbo."+store_name+" \n"
    elif store_name.lower() in ['kfa_select_data','kyw_select_data']:
        x="EXEC dbo."+store_name+" \n"
        x+="@IsHistory=?   ,@DateTo=? \n"
        x+=",@plant=?       ,@rank=?"
        if store_name.lower()=='kfa_select_data': y='SCCCLogistic'
        else: y='SCCC_Tracking'

    elif store_name.lower()=='search_orders_visibility':
        #x="DECLARE @out nvarchar(max); \n"
        x+='EXEC dbo.'+store_name+' \n'
        x+="@option =? ,@WO_DateFrom=?      ,@WO_DateTo=? \n"

        #x+=",@param_out=@out OUTPUT; \n"\
        #    "SELECT @out AS the_output;\n"
    else: print('Store procedure has no variable '+store_name)
    return y,x

def sql_execute_query(db_name,query,sql_var=None,is_print_query=False): #if no paremeter, set to None
    import pandas as pd
    #---1. Set parameters for query
    if query=='' or db_name=='': return None
    params=sql_var if sql_var is None or isinstance(sql_var,list) else list(sql_var.values()) #sql_var.items()
    #print(params)
    #---2. Print & execute query
    conn=connectDB(db_name)#.connect()
    cursor=conn.cursor()
    if params is None : rows=cursor.execute(query).fetchall()
    else :
        #print(query,params)
        rows=cursor.execute(query,params).fetchall() #SQL Server format-->#while rows: print(rows)
        query=check_sql_string(query,params)

    if is_print_query: print("------Execute query----- \n",query)
    #-------------------------------------
    columns=[column[0] for column in cursor.description] #must before move cursor
    df=pd.DataFrame.from_records(rows,columns=columns) #df=pd.read_sql(sql=query,con=conn,params=params)

    if is_print_query and cursor.nextset() :
        try: print('------Query -----',cursor.fetchall()[0][0])
        except Exception as e: print('error when print query',e)
    cursor.close()
    conn.close()
    df=filter_columns(df)
    return df

def connectDB(db_name):
    import pyodbc
    credentials=get_db_params(db_name)
    conn = pyodbc.connect('DRIVER={'+credentials['driver']+'};'                          'SERVER='+credentials['host']+';DATABASE='+credentials['database']+';'                          'UID='+credentials['username']+';PWD='+ credentials['password'])
                          #'autocommit=True')
#     if __name__ == "__main__": conn = conn #connect db
    if (conn==False): print ("Error, did not connect to the database")
    return conn

def get_db_params(db_name):
    if db_name in ['SCCC Data Warehouse']:
        credentials = { 'username'  : 'sa'
                        ,'password'  : 'Warehouse8*'
                        ,'host'      : '10.254.1.181'
                        ,'database'  : db_name
                        ,'driver'    : 'SQL Server'}
    elif db_name in ['SCCCLogisticsDataWarehouse']:
        credentials = { 'username'  : 'scccadm_dw'
                        ,'password'  : 'P@ssw0rd@1'
                        ,'host'      : 'sccclogistic.database.windows.net'
                        ,'database'  : db_name
                        ,'driver'    : 'SQL Server Native Client 11.0'}   #'ODBC Driver 11 for SQL Server'
    elif db_name in ['SCCCLogistic']:
        credentials = { 'username'  : 'scccadm'
                        ,'password'  : 'P@ssw0rd@1'
                        ,'host'      : 'sccclogistic.database.windows.net'
                        ,'database'  : db_name
                        ,'driver'    : 'SQL Server Native Client 11.0'}   #'ODBC Driver 11 for SQL Server'
    elif db_name=='SCCC_Tracking':
        credentials = { 'username'  : 'sccc_tracking'
                        ,'password'  : 'sccc_tracking'
                        ,'host'      : '210.1.60.112,1433'
                        ,'database'  : db_name
                        ,'driver'    : 'SQL Server'}
    return credentials


def check_sql_string(sql, values):
    unique = "%PARAMETER%"
    sql=sql.replace("?", unique)
    for v in values: sql = sql.replace(unique, repr(v), 1)
    return sql

def filter_columns(df):
    col_set=set(freight_columns2)
    user=ntpath.basename(os.environ['USERPROFILE'])
    if user.lower() not in freight_users:
        print('you user name:',user,'cannot see freight cost')
        col_set=set(freight_columns+freight_columns2)
    col_list=[x for x in df.columns if x not in col_set]
    return df[col_list]

def SearchOrderVisibility(woDate):

    """
    Create dataframe using seach order visibity query from SCCC data warehouse by WoDate and day minus
    select only GPS equal to 0, start extracting from woDate minus 90 days

    """

    store_name='search_orders_visibility'
    var=store_var(store_name)

    woDate = pd.to_datetime(woDate)
    woDateTo = woDate
    woDateFrom = woDateTo - datetime.timedelta(90)
    var['option'] = 2
    var['WO_DateFrom']= woDateFrom
    var['WO_DateTo']= woDateTo
    dbname,query=store_query(store_name)
    df=sql_execute_query(dbname,query,var,True)
    df = df[df['gps']==0]

    df['wo_date_from'] = pd.to_datetime(df['wo_date_from'],format ='%d/%m/%Y')
    df['wo_date_to'] = pd.to_datetime(df['wo_date_to'],format ='%d/%m/%Y')

    return df

def FindTruckIDAndShipment(woDate,ShipToCode):

    """
    Find TruckID by put in woDate and ShipToCode, look for data from OTM datawarehouse

    """

    dbname='SCCC Data Warehouse'

    woDate = pd.to_datetime(woDate)

    # plus one hours
    woDateTo = woDate + datetime.timedelta(2)

    query =  "SELECT * FROM ShipmentTracking WHERE [Weight Out Date by SH] between '"         + woDate.strftime('%Y-%m-%d %H:%M:%S')+"'" "and'"+ woDateTo.strftime('%Y-%m-%d %H:%M:%S')+"'        AND [ShipToCode] ='"+ShipToCode+"'" "and [Shipping Condition] like ('D%')"

    try:
        df =sql_execute_query(dbname,query,None,True)
        df = df[['ShipmentID','Transportation Zone','SoldToCode','SoldToName','ShipToCode','ShipToName','Truck ID','Weight Out Date by SH','ShippingPoint']]
    except:
        df = ""
    return df[0:1]

def CreateDfTracklog(License,woDate,dayPlus):

    """
    Create dataframe of GPS tracklog by input shipment number, truck license, woDate, and dateplus
    Select related columns and count frequency of GPS lat Lon wiht Tzone in four figures.

    """

    def TambonID():
        """ Call Tambon master"""
        dbname='SCCC Data Warehouse'
        query = "SELECT * FROM Master_Tambon"

        df = sql_execute_query(dbname,query,None,True)
        cols = ['CHANGWAT_E','AMPHOE_E','AM_ID']
        df = df[cols]
        df['CHANGWAT_E'] = df['CHANGWAT_E'].str.upper()
        df['AMPHOE_E'] = df['AMPHOE_E'].str.upper()
        df['AM_ID'] = df['AM_ID'].apply(str)
        df['AM_ID'] = df['AM_ID'].str.split(".").str[0]

        df.drop_duplicates(inplace = True)

        return df


    dbname='SCCCLogisticsDataWarehouse'
    woDate = pd.to_datetime(woDate)
    wo_dateFrom = woDate
    wo_dateTo = wo_dateFrom + datetime.timedelta(dayPlus)

    query =  "SELECT * FROM Tracklocations WHERE [datetime] between'" + wo_dateFrom.strftime('%Y-%m-%d') +"'and '" + wo_dateTo.strftime('%Y-%m-%d') +"'            AND [License] ='"+License+"'"

    df=sql_execute_query(dbname,query,None,True)

    cols = ['Latitude', 'Longtitude','Province','City','Town','GPSRefId']

    df = df[cols]

    df = df.pivot_table(values = 'GPSRefId',index = ['Latitude', 'Longtitude','Province','City','Town'],aggfunc = 'count').reset_index()

    df.rename(columns = {'GPSRefId': 'Freq'},inplace = True)

    df['Latitude'] = df['Latitude'].apply(str)
    df['Longtitude'] = df['Longtitude'].apply(str)

    dfT = TambonID()

    df = df.merge(dfT,how = 'left',left_on = ['Province','City'], right_on = ['CHANGWAT_E','AMPHOE_E'])

    return df

def DistanceGapKM(df):
    try:
        cor1 = (df['Latitude'],df['Longtitude'])
        cor2 = (df['LAT'],df['LON'])
        gap = distance.distance(cor1, cor2).km
    except:
        gap = ""
    return gap

def haversine(df):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    try:
        lat1 = pd.to_numeric(df['Latitude'], errors='coerce')
        lon1 = pd.to_numeric(df['Longtitude'], errors='coerce')
        lat2 = pd.to_numeric(df['LAT'], errors='coerce')
        lon2 = pd.to_numeric(df['LON'], errors='coerce')
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        # Radius of earth in kilometers is 6371
        gap = 6371* c

    except:
        gap = ""
    return gap

def haversine2(lat1,lon1,lat2=None,lon2=None,source=None):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    try:
        lat1 = pd.to_numeric(lat1, errors='coerce')
        lon1 = pd.to_numeric(lon1, errors='coerce')

        if source == '1012':
            lat2 = 16.442861
            lon2 = 102.796558
        else:
            lat2 = pd.to_numeric(lat2, errors='coerce')
            lon2 = pd.to_numeric(lon2, errors='coerce')
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        # Radius of earth in kilometers is 6371
        gap = 6371* c

    except:
        gap = ""
    return gap
# Start calculate

start = time.time()

today = pd.to_datetime('today') # find information from previous day

yesterday = today - datetime.timedelta(1) # Find shipto cann't close from yesterday minus 30 days


# Find truckID and shipment
df = SearchOrderVisibility(yesterday)

# seletct start-end range by select yesterday and two day before yesterday.

start_range = today - datetime.timedelta(3)
end_range = today - datetime.timedelta(1)

criteria = (df['wo_date_to'] >= start_range) & (df['wo_date_to']<= end_range)

df  = df[criteria]

# Create blank dataframe to collect info
dfFinal = pd.DataFrame()

for i in range(0,len(df)):
    dfF = FindTruckIDAndShipment(df.iloc[i]['wo_date_to'],df.iloc[i]['ShipToCode'])

    try:
        tzone = dfF.iloc[0]['Transportation Zone']
        shID = dfF.iloc[0]['ShipmentID']

    except:
        tzone = ""
        shID = "NotFound"+ str(df.iloc[i]['ShipToCode'])

    try:

        dfS = CreateDfTracklog(dfF.iloc[0]['Truck ID'],dfF.iloc[0]['Weight Out Date by SH'],2)

        # Filter only tzone match wiht GPS location

        dfS = dfS[dfS['AM_ID']==tzone]
        dfS['LAT'] = df.iloc[i]['LAT']
        dfS['LON'] = df.iloc[i]['LON']
        dfS.sort_values('Freq',ascending = False,inplace = True)
        dfS['ShipmentID'] = shID
        dfS['Lat+Lon'] = dfS['Latitude'] + "," + dfS["Longtitude"]
        dfF = dfF.merge(dfS,how = 'left',on='ShipmentID')
        dfFinal = dfFinal.append(dfF.iloc[0:1]) # number of row we want to extract
    except:
        dfFinal = dfFinal.append({'ShipmentID':shID},ignore_index = True)

#dfFinal.drop(['CHANGWAT_E','AMPHOE_E','AM_ID'],axis =1,inplace = True)
dfFinal = dfFinal[(dfFinal['ShippingPoint'] =='S41D') | (dfFinal['ShippingPoint']=='S44D')]

cols = ['ShipmentID','ShippingPoint','SoldToCode', 'SoldToName', 'ShipToCode', 'ShipToName','Province', 'City', 'Town','Transportation Zone','Truck ID','Weight Out Date by SH','LAT','LON','Latitude', 'Longtitude','Lat+Lon','Freq']

dfFinal = dfFinal[cols]

dfFinal['DistanceGapSys_GPS'] = dfFinal.apply(haversine,axis =1)

dfFinal.rename(columns={'LAT':'SysLAT','LON':'SysLON','Latitude':'GPS_LAT','Longtitude':'GPS_LON','Lat+Lon':'GPS_Lat+Lon'},inplace = True)

end = time.time()

active_dir=r"D:\Siam City Cement Public Company Limited\Logistics_Reports - Documents"
filename=r"\SuggestLatLon_"+start_range.strftime('%Y-%m-%d')+"_"+end_range.strftime('%Y-%m-%d')+".xlsx"
path_file=active_dir+filename

dfFinal.to_excel(path_file,index = False)
print("Total rows need to be investigated vs Total rows Found: {} / {} ".format(len(df),dfFinal['Freq'].count()))
print("Time to execute (min):",(end - start)/60)

#-----------------------------------------------------------------------------
import Tea as t
import datetime
import ctypes
issendmail=True

subject='GPS Lat Lon suggestion'
html="""<html>
        <header>Hi</header>
        <body>
        <p>Please look at """+subject+"""
        <br><br>*** This Email was auto generated from robot. Please do not reply. ***
        <br><br>Regards
        <br>SCCC Logistics
        </p></body></html>"""
href='<a href="'+path_file+'"> Click </a>'
html=html.format(href=href) #wb=excel.Workbooks.Open(excel_path) ws=wb.Worksheets(1) ws.Range("A1:B2").Copy() wb.Close()
maildict={}
maildict['msg_html']=html
maildict['subject']=('' if issendmail else 'TEST: ')+subject
maildict['contacts_file']=active_dir+r"\\"+('contacts.xlsx' if issendmail else 'contacts_.xlsx')
maildict['contacts_filter']='GpsLatLonSuggest'
maildict['attached_file']=path_file
mailtext=t.send_mail_yag(maildict) #mailtext=t.send_mail(maildict,'mail'if issendmail else 'mail')
#--------------------------------------------------------------------------------------------
his=t.history() #['subject','detail','FileOutput','error','InsertUser','SendMail','RunTime']
his.add([subject,subject,filename,'',mailtext])
his.save()

ctypes.windll.user32.MessageBoxW(0, "Complete calculation !!!", "Information", 1)


# In[ ]:

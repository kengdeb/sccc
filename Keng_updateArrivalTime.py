#!/usr/bin/env python
# coding: utf-8

import Tea as t
import pandas as pd


def selectPreDo(shNum, conn):
    """Input shipment number as text, database connenct cursor
    return ChildPreDO as text"""
    query = "SELECT PreDONo FROM ShipmentTracking WHERE [ShipmentID] = '"+shNum+"'"
    cursor = conn.cursor()
    cursor.execute(query)
    for row in cursor:
       return row[0]

def updatePreDO(PreDO,AATime,conn):
    """ Update datawarehouse Arive at Customer Date Time with data from excel"""
    query = "UPDATE ShipmentTracking SET [Arrive At Cust Date] ='"+AATime.strftime('%Y-%m-%d %H:%M:%S')+"'WHERE [PreDONo] = '"+PreDO+"'"
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()

def updateDW():
    
    dbname='SCCC Data Warehouse'
    conn = t.connectDB(dbname)
    
    df = pd.read_excel(r'C:\Users\nauetavo\Desktop\ReadThis.xlsx')
    col = ['Shipment ID','Destination', 'ArrivalTime']
    df= df[col]
    df = df.astype(str)
    df['ArrivalDate'] = df['Destination']+ " "+ df['ArrivalTime']
    df['ArrivalDate'] = pd.to_datetime(df['ArrivalDate'], format = '%Y-%m-%d %H:%M:%S')
    #print(df.info())
    #print(df)
    
    for row in range(len(df)):
        shNum  = df.iloc[row]['Shipment ID']
        AATime = df.iloc[row]['ArrivalDate']
        preDO = selectPreDo(shNum,conn)
        updatePreDO(preDO,AATime,conn)

if __name__ == '__main__':
    updateDW()

#print(selectPreDo("SH20200620-0114",cursor))

#updateSh('2410091405-10-00001','2020-06-22 00:00:00',conn)
import pandas as pd
from datetime import datetime,timedelta
import logging

def get_data(Trip_status,indent_status):
    # Trip_status=files[0]
    # indent_status=files[1]
    # Trip_status=pd.read_excel('Trip_status_report_1726214047574.xlsx')
    # indent_status=pd.read_excel('indent_status2024-08-24T_2024-09-13T_b57eeb274918d8794.xlsx')

    # Trip_status.head()
    try:
        if  set(['ShipmentID','Destination','Transporter','Reach ATA']).issubset(Trip_status.columns):
            logging.info(f'File {Trip_status} columns exist..')
        else:
            result={'success':False,'msg':'Trip_status file columns not exists..'}
            return f'Error: {result}'
    except Exception as e:
            # result={'success':False,'msg':'Column not exists..'}
            # print(f'File {Trip_status} column does not exist..')
            return f'Error: {str(e)}'

    # indent_status.head()
    try:
        if set(['Indent ID','Source Ref Id']).issubset(indent_status.columns):
            logging.info(f'File {indent_status} columns exist..')
        else:
            result={'success':False,'msg':'Indent_status file columns not exists..'}
            return f'Error: {result}'
    except Exception as e:
        logging.info(f'File {indent_status} column does not exist..')
        return f'Error: {str(e)}'
    Trip_status=Trip_status.dropna(subset=['Reach ATA'])
    Trip_status['Reach ATA']=pd.to_datetime(Trip_status['Reach ATA'],format='%d/%m/%y %I:%M %p').dt.date
    unique_date=Trip_status['Reach ATA'].unique()
    # Trip_status[['ShipmentID','Reach ATA']]

    current_time=pd.Timestamp.now().date()
    Date_difference=((current_time-Trip_status['Reach ATA']).apply(lambda x:x.days)-1)*1200
    Trip_status['date_difference']=Date_difference
    Trip_status_data=Trip_status.copy()

    for reach_ata in unique_date:
        Trip_status_data[reach_ata]=Trip_status_data.apply(
            lambda row:row['date_difference'] if row['Reach ATA'] ==reach_ata else 0,axis=1
        )

    Trip_status_data.columns=Trip_status_data.columns.astype(str)

    Trip_status_data=Trip_status_data[['ShipmentID','Destination','Transporter','2024-09-11','2024-09-12']]
    Indent_status_data=indent_status[['Indent ID','Source Ref Id']]

    Indent_status_data=Indent_status_data.copy()
    Indent_status_data.rename(columns={'Indent ID':'ShipmentID'},inplace=True)

    need_column=pd.merge(Trip_status_data,Indent_status_data,how='inner',on='ShipmentID')
    need_column.rename(columns={'Source Ref Id':'Source'},inplace=True)

    df=need_column[['Source','2024-09-12','2024-09-11']]
    df1=need_column[['Source','Destination','Transporter','2024-09-11','2024-09-12']]
    # df.to_excel('Shipment_id.xlsx')
    # df1.to_excel('Transport.xlsx')
    logging.info(f'line 59')
    json_format=df.to_json(orient='records')
    logging.info(f'line 61')
    json_format2=df1.to_json(orient='records')
    logging.info(f'line 64')
    return json_format,json_format2
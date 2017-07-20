'''
current version of this code takes a google sheet, copy of original which is static... next thing would be to
implement a copy in google drive... I can go do that now...

total time is --- 149.125 seconds ---

'''
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time
from spreadsheets.url_keeper import url_form_test
start_timez = time.time()

import json
from pprint import pprint

#with open('C:/Users/585000/Desktop/Python Projects/PPM USAID/spreadsheets/client_secret_2.json') as data_file:
#    data = json.load(data_file)

#pprint(data)

scope = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/585000/Desktop/Python Projects/PPM USAID/spreadsheets/client_secret_2.json', scope)

gc = gspread.authorize(credentials)

wks = gc.open_by_url(url_form_test()).get_worksheet(0)

internal_wks = gc.open_by_url(url_form_test()).get_worksheet(1)

external_wks = gc.open_by_url(url_form_test()).get_worksheet(2)

change_sheet = pd.DataFrame()
internal_sheet = pd.DataFrame()
external_sheet = pd.DataFrame()

for i in range(22):
    n = i+1
    print n
    column = pd.DataFrame([sub for sub in wks.col_values(n)])
    column.columns = column.iloc[0]
    column = column.ix[1:]
    change_sheet = pd.concat([change_sheet, column], axis=1)

    del column

    column = pd.DataFrame([sub for sub in internal_wks.col_values(n)])
    column.columns = column.iloc[0]
    column = column.ix[1:]
    internal_sheet = pd.concat([internal_sheet, column], axis=1)

    del column

    column = pd.DataFrame([sub for sub in external_wks.col_values(n)])
    column.columns = column.iloc[0]
    column = column.ix[1:]
    external_sheet = pd.concat([external_sheet, column], axis=1)

    del column

def checkEqual1(iterator):
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == rest for rest in iterator)

# need to deal with duplicate coluimns Orion Field Name
col_list_change_sheet = ['Timestamp',
 'Email Address',
 "Requester's Name",
 'Reason for change',
 'Program',
 'Do you need bulk changes?',
 'Please upload file with bulk changes',
 'PE No',
 'PQ No',
 'Order No',
 'Shipment No',
 'What type of change is needed?',
 'Orion field name',
 'Current date',
 'Requested new date',
 'Orion field name_2',
 'Current data',
 'Requested new data',
 'Correction ID',
 'Date approved by PMU',
 'Global Fund or Internal tab?',
 'Status of correction by ORION']

change_sheet.columns = col_list_change_sheet

drop_list = []
for indexz, row in change_sheet.iterrows():
    if checkEqual1(row.tolist()) == True:
        drop_list.append(indexz-1)

change_sheet=change_sheet.drop(change_sheet.index[drop_list])

drop_list = []
for indexz, row in internal_sheet.iterrows():
    if checkEqual1(row.tolist()) == True:
        drop_list.append(indexz-1)

internal_sheet = internal_sheet.drop(internal_sheet.index[drop_list])

drop_list = []
for indexz, row in external_sheet.iterrows():
    if checkEqual1(row.tolist()) == True:
        drop_list.append(indexz-1)


change_sheet = change_sheet.rename(index={15:'Orion field name_2'})

external_sheet = external_sheet.drop(external_sheet.index[drop_list])
external_sheet = external_sheet.drop('',axis=1)

change_sheet_c = change_sheet.copy()
additional_blank_cols = ['AD/UD Code',
 'Number of days (+/-)',
 'AD/UD Comments',
 'Current AD',
 'Current UD',
 'Actual Delivery Date',
 'COTD impact % (+/-)','Date change approved by TGF?',
 'Shared with Client?'
                         ]

for col in additional_blank_cols:
    change_sheet_c[col]= ''
done_sheet = change_sheet_c[change_sheet_c['Status of correction by ORION'].str.lower()=='done']
internal_edits =done_sheet[done_sheet['Global Fund or Internal tab?'].str.lower().str.contains('internal only')==True]
external_edits =done_sheet[done_sheet['Global Fund or Internal tab?'].str.lower().str.contains('internal only')!=True]


external_edits = external_edits.rename(index = str,
                                       columns={'Timestamp':'Date of request',
                                                'Date approved by PMU':'Date Done', #seems not like a 1:1 ratio
                                                'Reason for change':'Reason for change', #same but for good measure
                                                'Program':'Program', #''
                                                'PE No':'PE No',
                                                'PQ No':'PQ No',
                                                'Order No':'Order No',
                                                'Shipment No':'Shipment No',
                                                'Orion field name':'Field Name associated with correction',
                                                'Current date':'Existing Data (Orion)',
                                                'Requested new date':'New Data (Revised)',
                                                'Status of correction by ORION':'Done'
                                       }
                                       )

external_edits = external_edits[['Date of request',
 'Date Done',
 'Reason for change',
 'Program',
 'PE No',
 'PQ No',
 'Order No',
 'Shipment No',
 'Field Name associated with correction',
 'Existing Data (Orion)',
 'New Data (Revised)',
 'AD/UD Code',
 'Number of days (+/-)',
 'AD/UD Comments',
 'Current AD',
 'Current UD',
 'Actual Delivery Date',
 'COTD impact % (+/-)',
 'Done',
 'Date change approved by TGF?',
 'Shared with Client?']]

external_result = pd.concat([external_sheet,external_edits])



internal_edits = internal_edits.rename(index = str,
                                       columns={'Timestamp':'Date of request',
                                                'Date approved by PMU':'Date Done', #seems not like a 1:1 ratio
                                                'Reason for change':'Reason for Correction', #same but for good measure
                                                'Program':'Program', #''
                                                'PE No':'PE No',
                                                'PQ No':'PQ No',
                                                'Order No':'Order No',
                                                'Shipment No':'Shipment No',
                                                'Orion field name':'Field Name associated with correction',
                                                'Current date':'Existing Data (Orion)',
                                                'Requested new date':'New Data (Revised)',
                                                'Status of correction by ORION':'Status'
                                       }
                                       )

internal_edits['Reason for change'] = ''


internal_col_list =['Date of request',
 'Date Done',
 'Reason for Correction',
 'Program',
 'PE No',
 'PQ No',
 'Order No',
 'Shipment No',
 'Field Name associated with correction',
 'Existing Data (Orion)',
 'New Data (Revised)',
 'AD/UD Code',
 'Number of days (+/-)',
 'AD/UD Comments',
 'Current AD',
 'Current UD',
 'Actual Delivery Date',
 'COTD impact % (+/-)',
 'Reason for change',
 'Status',
 'Date change approved by TGF?',
 'Shared with Client?']

#df a dataframe and ws a google api worksheet object
def to_googlesheet(df,ws):

    def numberToLetters(q):
        q = q - 1
        result = ''
        while q >= 0:
            remain = q % 26
            result = chr(remain+65) + result;
            q = q//26 - 1
        return result

    # columns names
    columns = df.columns.values.tolist()
    # selection of the range that will be updated
    cell_list = ws.range('A1:'+numberToLetters(len(columns))+'1')
    # modifying the values in the range
    for cell in cell_list:
        val = columns[cell.col-1]
        if type(val) is str:
            val = val.decode('utf-8')
        cell.value = val
    # update in batch
    ws.update_cells(cell_list)

    # number of lines and columns
    num_lines, num_columns = df.shape
    # selection of the range that will be updated
    cell_list = ws.range('A2:' + numberToLetters(num_columns) + str(num_lines + 1))
    # modifying the values in the range
    for cell in cell_list:
        val = df.iloc[cell.row - 2, cell.col - 1]
        if type(val) is str:
            val = val.decode('utf-8')
        elif isinstance(val, (int, long, float, complex)):
            # note that we round all numbers
            val = int(round(val))
        cell.value = val
    # update in batch
    ws.update_cells(cell_list)

internal_edits = internal_edits[internal_col_list]

internal_result = pd.concat([internal_sheet,internal_edits])


internal_save_loc = gc.open_by_url(url_form_test()).get_worksheet(3)
external_save_loc = gc.open_by_url(url_form_test()).get_worksheet(4)

internal_save_loc.clear()
external_save_loc.clear()

to_googlesheet(internal_result,internal_save_loc)
to_googlesheet(external_result,external_save_loc)


#external_result.to_csv('C:/Users/585000/Desktop/PCFSM/2017 KPIs/external_change_test_v1.csv',index = False)
#internal_result.to_csv('C:/Users/585000/Desktop/PCFSM/2017 KPIs/internal_change_test_v1.csv',index = False)

print("total time --- %s seconds ---" % (time.time() - start_timez))

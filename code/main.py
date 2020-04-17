import requests
import pandas as pd
import numpy as np
import os

def download_ARB():
    url="""https://ww3.arb.ca.gov/cc/capandtrade/offsets/issuance/arboc_issuance.xlsx"""

    resp = requests.get(url)
    with open('data/ARB.xls', 'wb') as output:
        output.write(resp.content)

    return None

def load_excel():
    return pd.read_excel(open('data/ARB.xls', 'rb'), sheet_name='ARB Offset Credit Issuance', header=0)

def get_location_data():
    df = load_excel()

    statelist = []
    locallist = []
    countrylist = []
    for idx, prj in df.iterrows():
        if idx % 10 == 0: print(idx)
        if 'thereserve2.apx.com' in  prj['Project Documentation']:
            req = requests.get(prj['Project Documentation'])
            df2 = pd.read_html( req.text )[0]            
            statelist.append(  df2.loc[df2.iloc[ :, 0] == 'State/Province:', 4 ].iloc[0] )
            locallist.append( df2.loc[df2.iloc[:, 0] == 'Project Site Location:', 4].iloc[0] )
            countrylist.append( df2.loc[df2.iloc[:,0] == 'Country:', 4].iloc[0])
        elif 'acr2.apx.com' in prj['Project Documentation']:
            req = requests.get(prj['Project Documentation'])
            df2 = pd.read_html( req.text )[0]            
            statelist.append(  df2.loc[df2.iloc[ :, 0] == 'Project Site State (Primary):', 4 ].iloc[0] )
            locallist.append( df2.loc[df2.iloc[:, 0] == 'Project Site Location:', 4].iloc[0] )
            countrylist.append( df2.loc[df2.iloc[:,0] == 'Project Site Country:', 4].iloc[0])           

        else:
            statelist.append(None)
            locallist.append(None)
            countrylist.append(None)

    df.loc[ :, 'state_from_url'] = pd.Series( statelist )
    df.loc[ :, 'location_from_url'] = pd.Series( locallist )
    df.loc[ :, 'country_from_url'] = pd.Series( countrylist )

    df.to_csv('data/ARB_with_local.tdf', sep='\t', index=False)

def add_lat_long():
    apikey = os.environ['GKEY']

    base_url = f'https://maps.googleapis.com/maps/api/geocode/json?key={apikey}&address='
    df = pd.read_csv('data/ARB_with_local.tdf', sep='\t')
    

    latlist = []
    lnglist = []
    bloblist = []

    for idx, prj in df.iterrows():
        if idx % 10 == 0 : print(idx)
        if not isinstance(prj['location_from_url'], float): 
            ### The above condition checks for missings
            ### when they are loaded the are loaded as floats
            ### should change the read to force to str

            tolookup = '+'.join( [
                prj['location_from_url'].replace(' ', '+')
                , prj['state_from_url'].replace(' ', '+')
                , prj['country_from_url'].replace(' ', '+')
                ])
            req = requests.get( base_url + tolookup).json()['results'][0]

            latlist.append( req['geometry']['location']['lat'] )
            lnglist.append( req['geometry']['location']['lng'] )
            bloblist.append( req )

        else:
            latlist.append(None)
            lnglist.append(None)
            bloblist.append(None)

    df.loc[ :, 'lat'] = pd.Series(latlist)
    df.loc[ :, 'lng'] = pd.Series(lnglist)
    df.loc[ :, 'blob'] = pd.Series(bloblist)

    df.to_csv('data/ARB_with_lat_long.tdf', sep='\t', index=False)

if __name__ == '__main__':
    #download_ARB()
    #load_excel()
    #get_location_data()
    add_lat_long()
import pymssql
import pandas as pd
import numpy as np
import yaml
import io
import os

from pull_from_pdb import get_pdb_data
from pull_from_sharepoint import get_sharepoint_data

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

## pull from pdb/sharepoint, save to sharepoint ------------------------------##
def main():
    pdb_data = get_pdb_data()
    sharepoint_data = get_sharepoint_data()

    data = sharepoint_data.merge(pdb_data, on=['network','protocol'], how='outer')
    data = data.drop(columns=['name','ProtocolName','ProtocolId'])

    data['protocol_name'] = (data.network + " " + data.protocol).map(name_map)

    usecols = [
        'protocol_name',
        'cap_version',
        'last_cap_revision_date',
        'stage_of_protocol_operations',
        'target_or_actual_open_date',
    ]
    data = data[usecols]

    def refine_cap_version(x):
        if 'V0.0 OR ' in x:
            return x[8:-1]
        if x=='No version number found':
            return 'Error'
        return x

    data.cap_version = data.cap_version.apply(refine_cap_version)

    def refine_last_cap_revision_date(x):
        if x=="NA":
            return "Error"
        else:
            return x

    data.last_cap_revision_date = data.last_cap_revision_date.map(refine_last_cap_revision_date)

    def refine_target_or_actual_open_date(x):
        if x=="NaT":
            return "Missing from PBD"
        else:
            return x

    data.target_or_actual_open_date = data.target_or_actual_open_date.astype(str)
    data.target_or_actual_open_date = data.target_or_actual_open_date.map(refine_target_or_actual_open_date)
    save_to_sharepoint(data)

## ---------------------------------------------------------------------------##
def save_to_sharepoint(data):
    yaml_path = "/home/bhaddock/repos/sdmc_cap_tracker/config.yaml"
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    username_shrpt = 'bhaddock@fredhutch.org'
    password_shrpt = config['password']

    url = 'https://fredhutch-my.sharepoint.com/personal/bhaddock_fredhutch_org/'
    folder = '/personal/bhaddock_fredhutch_org/Documents/Documents/sharepoint_stopover'

    ctx_auth = AuthenticationContext(url)
    if ctx_auth.acquire_token_for_user(username_shrpt, password_shrpt):
      ctx = ClientContext(url, ctx_auth)
      web = ctx.web
      ctx.load(web)
      ctx.execute_query()
      print('Authenticated into sharepoint as: ',web.properties['Title'])

    else:
      print(ctx_auth.get_last_error())

    towrite = io.BytesIO()
    data.to_excel(towrite)
    towrite.seek(0)

    remotepath = '/personal/bhaddock_fredhutch_org/Documents/Documents/sharepoint_stopover/CAP_pulled_data.xlsx'
    dir, name = os.path.split(remotepath)

    file_content = towrite.getvalue()

    file = ctx.web.get_folder_by_server_relative_url(dir).upload_file(name, file_content).execute_query()

## constants -----------------------------------------------------------------##
name_map = {
    'CoVPN 3008': 'CoVPN 3008',
    'CoVPN 5001': 'CoVPN 5001',
    'HVTN 115': 'HVTN 115',
    'HVTN 128': 'HVTN 128',
    'HVTN 135': 'HVTN 135',
    'HVTN 137': 'HVTN 137',
    'HVTN 139': 'HVTN 139',
    'HVTN 141': 'HVTN 141',
    'HVTN 144': 'HVTN 144',
    'HVTN 300': 'HVTN 300',
    'HVTN 301': 'HVTN 301',
    'HVTN 302': 'HVTN 302',
    'HVTN 303': 'HVTN 303',
    'HVTN 304': 'HVTN 304',
    'HVTN 305': 'HVTN 305',
    'HVTN 307': 'HVTN 307',
    'HVTN 309': 'HVTN 309',
    'HVTN 310': 'HVTN 310',
    'HVTN 312': 'HVTN 312',
    'HVTN 317': 'HVTN 317',
    'HVTN 318': 'HVTN 318',
    'HVTN 405': 'HVTN 405',
    'HVTN 606': 'HVTN 606',
    'HVTN 807': 'HVTN 807',
    'CoVPN 3008 subclinical TB study': 'CoVPN 3008_TB_Substudy',
    'HVTN 123': 'HVTN 123',
    'HVTN 136': 'HVTN 136/HPTN 092',
    'HVTN 140': 'HVTN 140/HPTN 101',
    'HVTN 143': 'HVTN 143',
    'HVTN 306': 'HVTN 306',
    'HVTN 311': 'HVTN 311',
    'HVTN 313': 'HVTN 313 (Former 308)',
    'HVTN 315': 'HVTN 315 (426c)',
    'HVTN 316': 'HVTN 316 (pediatric 426c)',
    'HVTN 319': 'HVTN 319 (UFOSApNP)',
    'HVTN 320': 'HVTN 320 (HxB2 WT)',
    'HVTN 603': 'HVTN 603/A5397',
    'HVTN 605': 'HVTN 605/A5421',
    'HVTN 804': 'HVTN 804/HPTN 095',
    'HVTN 805': 'HVTN 805/HPTN 093',
    'HVTN 806': 'HVTN 806/HPTN 108/A5416'
}

if __name__=="__main__":
    main()

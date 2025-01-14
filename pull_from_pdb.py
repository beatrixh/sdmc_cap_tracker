## --------------------------------------------------------------------##
# Author: Beatrix Haddock
# Date: 2024-09-17
# Purpose:
# Pull from PDB:
#   - stage of protocol operations
#   - status of protocol
#   - date of target or actual protocol open
#   - date of target or actual first enrollment
#   - date of target or actual enrollment complete
#   - date of target or actual enrollment complete
## --------------------------------------------------------------------##
import pymssql
import pandas as pd
import numpy as np
import yaml

def get_pdb_data():
    ## pull list of protocols interested in ---------------------------##
    cap_list = pd.read_csv("/home/bhaddock/repos/sdmc_cap_tracker/cap_sharepoint_links.txt", usecols=['network','protocol'], sep="\t")
    cap_list['ProtocolName'] = cap_list.network + cap_list.protocol

    ## password -------------------------------------------------------##
    yaml_path = "/home/bhaddock/repos/sdmc_cap_tracker/config.yaml"
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    ## pull data from pdb ---------------------------------------------##
    conn = pymssql.connect(
        host=r'sqlprdaz01',
        user=r'FHCRC\bhaddock',
        password=config['password'],
        database='CDS_PDB_Prod'
    )

    cursor = conn.cursor(as_dict=True)

    cursor.execute("""SELECT * FROM dbo.tblProtocol""")
    PROTOCOL_DATA = pd.DataFrame(cursor.fetchall())

    cursor.execute("""SELECT * FROM dbo.tblProtocolMilestone""")
    MILESTONE_DATA = pd.DataFrame(cursor.fetchall())

    cursor.execute("""SELECT * FROM dbo.tblProtocolStage""")
    STAGE_METADATA = pd.DataFrame(cursor.fetchall())

    cursor.execute("""SELECT * FROM dbo.tblProtocolMilestoneList""")
    MILESTONE_METADATA = pd.DataFrame(cursor.fetchall())

    conn.commit()

    ## merge on Protocol Id column ------------------------------------##
    def find_corresponding(name, current_only=True):
        if 'TB' in name:
            options = PROTOCOL_DATA.loc[(PROTOCOL_DATA.ProtocolName.str.contains("TB"))]
        else:
            options = PROTOCOL_DATA.loc[~(PROTOCOL_DATA.ProtocolName.str.contains("TB")) & (PROTOCOL_DATA.ProtocolName.str.replace(" ","").str.contains(name))]
        if current_only:
            options = options.loc[options.CurrentVersion]
        options = [i for i in options.ProtocolName if 'z' not in i]
        options = np.sort(options)
        return options[-1]

    cap_list.ProtocolName = cap_list.ProtocolName.apply(find_corresponding)
    protocol_ids = PROTOCOL_DATA.loc[PROTOCOL_DATA.ProtocolName.isin(cap_list.ProtocolName)].ProtocolId.unique().tolist()

    ## PULL AND FORMAT MILESTONE DATA ---------------------------------##
    # protocol open = 300
    # first ppt enrolled = 320
    # enrollment complete = 380
    # follow up complete = 400


    # grab milestones
    r = MILESTONE_DATA[['ProtocolID',
                                 'ProtocolMilestoneListId',
                                 'MilestoneTargetStartDate',
                                 'MilestoneStartDate',
                                 'MilestoneTargetEndDate',
                                 'MilestoneEndDate']]

    # merge on milestone names
    r = r.merge(MILESTONE_METADATA[['ProtocolMilestoneListId', 'MilestoneId', 'ProtocolMilestoneName']].drop_duplicates(),
                                  on='ProtocolMilestoneListId',
                                  how = 'left')

    # merge on protocol names
    r = r.merge(PROTOCOL_DATA[['ProtocolId','ProtocolName']],
                                  left_on='ProtocolID',
                                  right_on='ProtocolId',
                                  how='left')
    r = r[['ProtocolId',
                             'ProtocolName',
                             'ProtocolMilestoneListId',
                             'MilestoneId',
                             'ProtocolMilestoneName',
                             'MilestoneTargetStartDate',
                             'MilestoneStartDate',
                             'MilestoneTargetEndDate',
                             'MilestoneEndDate']]

    t = r.loc[r.ProtocolId.isin(protocol_ids)].sort_values(by=['ProtocolId','ProtocolMilestoneListId'])

    ##  start reshaping data ------------------------------------------##

    # subset to 'Protocol open'
    t = t.loc[t.MilestoneId==300]

    # milestone time points to long
    t = t.melt(
        id_vars=['ProtocolId','ProtocolName','ProtocolMilestoneListId','ProtocolMilestoneName'],
        value_vars=['MilestoneTargetStartDate', 'MilestoneStartDate', 'MilestoneTargetEndDate', 'MilestoneEndDate'],
        var_name='milestone_timept',
        value_name='dt',
    )
    t.ProtocolMilestoneListId = 'p' + t.ProtocolMilestoneListId.astype(str)

    # different listids as columns
    t = pd.pivot_table(
        t,
        index=['ProtocolId','ProtocolName','ProtocolMilestoneName','milestone_timept'],
        columns='ProtocolMilestoneListId',
        values='dt'
    ).reset_index()

    # if 4040 available and 300 missing, take 4040
    t.loc[t.p300.isna(), 'p300'] = t.loc[t.p300.isna(), 'p4040']

    # want to take actual start if available; target if not
    t['Target'] = t.milestone_timept.str.contains("Target").map({True:'Target', False:'Actual'})
    t['Point'] = t.milestone_timept.str.contains("Start").map({True:'Start', False:'End'})

    t = pd.pivot_table(
        t,
        index=['ProtocolId','ProtocolName','ProtocolMilestoneName', 'Target'],
        columns='Point',
        values='p300'
    ).reset_index()

    t['use'] = t.Start
    t.loc[(t.Start.isna()), 'use'] = t.loc[(t.Start.isna()), 'End']

    t = pd.pivot_table(
        t.drop(columns=['End','Start']),
        index=['ProtocolId', 'ProtocolName', 'ProtocolMilestoneName'],
        columns='Target'
    ).droplevel(level=0, axis=1).reset_index()

    t.loc[t.Actual.isna(), 'Actual'] = t.loc[t.Actual.isna(), 'Target']
    t['Protocol open'] = t.Actual

    # subset to result columns
    t = t[['ProtocolId', 'ProtocolName', 'Protocol open']]

    ## Pull and format stage/status -----------------------------------##
    s = PROTOCOL_DATA[['ProtocolId','ProtocolName','ProtocolStage']].merge(
        STAGE_METADATA[['ProtocolStageId','StageName','StatusName']],
        left_on='ProtocolStage',
        right_on='ProtocolStageId'
    )
    s = s.drop(columns=['ProtocolStage', 'ProtocolStageId'])
    s = s.loc[s.ProtocolId.isin(protocol_ids)]


    ## Concat and format all data -------------------------------------##
    final = s.merge(t, on=['ProtocolId','ProtocolName'], how='outer')

    renaming = {
    'StageName': 'stage_of_protocol_operations',
    'StatusName': 'protocol_status',
    'Protocol open': 'target_or_actual_open_date'
    }

    final = final.rename(columns=renaming)
    final = cap_list.merge(final, on="ProtocolName")

    ## Fill in missing dates with earlier versions --------------------##
    missing_protocols = final.loc[final.target_or_actual_open_date.isna()].ProtocolId.tolist()

    def get_previous_versions(ProtocolId):
        s = PROTOCOL_DATA.loc[PROTOCOL_DATA.ProtocolId==ProtocolId]
        root_id = s.RootProtocolId.values[0]
        version = s.Version.values[0]
        versions = []
        for v in range(version-1,0,-1):
            last_id = PROTOCOL_DATA.loc[(PROTOCOL_DATA.RootProtocolId==root_id) & (PROTOCOL_DATA.Version==v)].ProtocolId.values[0]
            versions += [last_id]
        return versions

    missing_protocols = final.loc[final.target_or_actual_open_date.isna()].ProtocolId.tolist()

    milestone_timept_cols = ['MilestoneStartDate',
                             'MilestoneTargetStartDate',
                             'MilestoneEndDate'
                             'MilestoneTargetEndDate',
                            ]

    def get_latest_available_protocol_open_date(ProtocolId):
        prev_versions = get_previous_versions(ProtocolId)
        for v in prev_versions:
            protocol_open = r.loc[(r.ProtocolId==v) & (r.MilestoneId==300)]
            if len(protocol_open) > 0:
                for timept in milestone_timept_cols:
                    date = r.loc[(r.ProtocolId==v) & (r.MilestoneId==300),timept].values[0]
                    if pd.notnull(date):
                        return date
        return pd.NaT

    for p in missing_protocols:
        final.loc[final.ProtocolId==p, 'target_or_actual_open_date'] = get_latest_available_protocol_open_date(p)

    return final

if __name__=="__main__":
    final = get_pdb_data()

    savedir = '/networks/vtn/lab/SDMC_labscience/operations/projects/CAP_projectfiles/project_management_ideas/'
    today = datetime.date.today().isoformat()
    final.to_excel(savedir + f"protocol_data_from_pdb_{today}.xlsx")

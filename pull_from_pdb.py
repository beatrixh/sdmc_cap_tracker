## ---------------------------------------------------------------------------##
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
## ---------------------------------------------------------------------------##
import pymssql
import pandas as pd
import numpy as np
import yaml

def get_pdb_data():
    ## pull list of protocols interested in ----------------------------------##
    cap_list = pd.read_csv("/home/bhaddock/repos/sdmc_cap_tracker/cap_sharepoint_links.txt", usecols=['network','protocol'], sep="\t")
    cap_list['ProtocolName'] = cap_list.network + cap_list.protocol

    ## password --------------------------------------------------------------##
    yaml_path = "/home/bhaddock/repos/sdmc_cap_tracker/config.yaml"
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    ## pull data from pdb ----------------------------------------------------##
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

    ## merge on Protocol Id column -------------------------------------------##
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

    ## PULL AND FORMAT MILESTONE DATA ----------------------------------------##
    # protocol open = 300
    # first ppt enrolled = 320
    # enrollment complete = 380
    # follow up complete = 400


    # grab milestones
    t = MILESTONE_DATA[['ProtocolID',
                                 'ProtocolMilestoneListId',
                                 'MilestoneTargetStartDate',
                                 'MilestoneStartDate',
                                 'MilestoneTargetEndDate',
                                 'MilestoneEndDate']]

    # merge on milestone names
    t = t.merge(MILESTONE_METADATA[['ProtocolMilestoneListId','ProtocolMilestoneName']].drop_duplicates(),
                                  on='ProtocolMilestoneListId',
                                  how = 'left')

    # merge on protocol names
    t = t.merge(PROTOCOL_DATA[['ProtocolId','ProtocolName']],
                                  left_on='ProtocolID',
                                  right_on='ProtocolId',
                                  how='left')
    t = t[['ProtocolId',
                             'ProtocolName',
                             'ProtocolMilestoneListId',
                             'ProtocolMilestoneName',
                             'MilestoneTargetStartDate',
                             'MilestoneStartDate',
                             'MilestoneTargetEndDate',
                             'MilestoneEndDate']]

    t = t.loc[t.ProtocolId.isin(protocol_ids)].sort_values(by=['ProtocolId','ProtocolMilestoneListId'])

    t = t.loc[(t.ProtocolMilestoneListId.isin([300,320,380,400]))].sort_values(by=['ProtocolId',
                                                                                                     'ProtocolMilestoneListId'])
    t = t.melt(
        id_vars=['ProtocolId','ProtocolName','ProtocolMilestoneListId','ProtocolMilestoneName'],
        value_vars=['MilestoneTargetStartDate', 'MilestoneStartDate', 'MilestoneTargetEndDate', 'MilestoneEndDate'],
        var_name='milestone_timept',
        value_name='dt',
    )
    t['Target'] = t.milestone_timept.str.contains("Target").map({True:'Target', False:'Actual'})
    t['Point'] = t.milestone_timept.str.contains("Start").map({True:'Start', False:'End'})

    t = pd.pivot_table(t,
                   index=['ProtocolId','ProtocolName','ProtocolMilestoneListId','ProtocolMilestoneName', 'Target'],
                   columns='Point',
                   values='dt'
                  ).reset_index()

    t['use'] = t.Start
    cond = (t.Start.isna()) & (t.End.notna())
    t.loc[(t.ProtocolMilestoneListId==300) & cond, 'use'] = t.loc[(t.ProtocolMilestoneListId==300) & cond, 'End']
    t.loc[(t.ProtocolMilestoneListId==320) & cond, 'use'] = t.loc[(t.ProtocolMilestoneListId==320) & cond, 'End']
    t.loc[(t.ProtocolMilestoneListId==380) & cond, 'use'] = t.loc[(t.ProtocolMilestoneListId==380) & cond, 'End']
    t.loc[(t.ProtocolMilestoneListId==400) & cond, 'use'] = t.loc[(t.ProtocolMilestoneListId==400) & cond, 'End']
    t = t.drop(columns=['Start','End'])

    t = pd.pivot_table(t, index=['ProtocolId', 'ProtocolName', 'ProtocolMilestoneListId',
           'ProtocolMilestoneName'], columns='Target').droplevel(level=0, axis=1).reset_index()

    t['use'] = t.Actual
    t.loc[t.Actual.isna() & t.Target.notna(), 'use'] = t.loc[t.Actual.isna() & t.Target.notna(), 'Target']

    t = pd.pivot_table(t, index=['ProtocolId', 'ProtocolName'], columns='ProtocolMilestoneName', values='use').reset_index()

    ## Pull and format stage/status ------------------------------------------##
    s = PROTOCOL_DATA[['ProtocolId','ProtocolName','ProtocolStage']].merge(
        STAGE_METADATA[['ProtocolStageId','StageName','StatusName']],
        left_on='ProtocolStage',
        right_on='ProtocolStageId'
    )
    s = s.drop(columns=['ProtocolStage', 'ProtocolStageId'])
    s = s.loc[s.ProtocolId.isin(protocol_ids)]


    ## Concat and format all data --------------------------------------------##
    final = s.merge(t, on=['ProtocolId','ProtocolName'], how='outer')

    renaming = {
        'StageName': 'stage_of_protocol_operations',
        'StatusName': 'protocol_status',
        'Enrollment complete': 'target_or_actual_enrollment_complete',
        'First participant enrolled': 'target_or_actual_first_enrollment_date',
        'Follow-up complete': 'target_or_actual_followup_complete',
        'Protocol open': 'target_or_actual_open_date'
    }

    final = final.rename(columns=renaming)
    final = cap_list.merge(final, on="ProtocolName")

    return final

if __name__=="__main__":
    final = get_pdb_data()

    savedir = '/networks/vtn/lab/SDMC_labscience/operations/projects/CAP_projectfiles/project_management_ideas/'
    today = datetime.date.today().isoformat()
    final.to_excel(savedir + f"protocol_data_from_pdb_{today}.xlsx")

import pandas as pd
import json
import os

data_folder = "../../../data/betfair/soccer/BASIC/2022/Jan"
# data_folder = "../../../data/betfair/soccer/BASIC/2022/Jan/1"
output_folder = "../../../data/clean"
meta_infos = []

for path, subdirs, files in os.walk(data_folder):
    for file in files:
        full_path = f"{path}/{file}"
        objList = []
        with open(full_path) as f:
            event_name = ""
            market_name = ""
            runner_id_to_name = {}
            for i, jsonObj in enumerate(f):
                objDict = json.loads(jsonObj)
                if i == 0:
                    if len(objDict['mc']) == 1 and 'marketDefinition' in objDict['mc'][0]:
                        key_to_check = ['eventName', 'name']
                        if all(key in objDict['mc'][0]['marketDefinition'] for key in key_to_check):
                            meta_info = {
                                "event_name": objDict['mc'][0]['marketDefinition']['eventName'],
                                "market_name": objDict['mc'][0]['marketDefinition']['name']
                            }
                            meta_infos.append(meta_info)
                            for runner in objDict['mc'][0]['marketDefinition']['runners']:
                                runner_id_to_name[runner['id']] = runner['name']
                        else:
                            raise "Wrong format"
                    else:
                        raise "Wrong format"
                else:
                    pass
                    # if 'rc' in objDict:
                    #     for rc in objDict['rc']:
                    #         ltp = rc['ltp']
                    #         name = rc[runner_id_to_name[rc['id']]]

df = pd.DataFrame(meta_infos)
df.to_csv(f'{output_folder}/events.csv', index=False, header=True, mode="w")


df = df.drop(columns=['market_name'])
df = df.drop_duplicates()
df.to_csv(f'{output_folder}/games.csv', index=False, header=True, mode="w")
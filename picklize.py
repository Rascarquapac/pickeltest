import pandas as pd
import csv
import re
import pickle 
import pandas as pd
import os
import requests
import sys

def picklize_cameras():
    cameras = pd.read_csv("./csv/CyanviewDescriptor-Cameras.csv",usecols=['Model','Reference','Protocol','Brand','ManufacturerURL','Remark'])
    cam_df  = pd.DataFrame(cameras)
    try:
        columns = cam_df.columns[cam_df.columns.duplicated(keep=False)]
        rows = cam_df.index[cam_df.index.duplicated(keep=False)]
        if not columns.empty :
            print("Duplicated Columns :\n",columns)
            raise Exception('Duplicated Columns in CyanviewDescriptor-Cameras.csv')
        if not rows.empty :
            print("Duplicated Rows :\n",rows)
            raise Exception('Duplicated Rows in CyanviewDescriptor-Cameras.csv')
    except Exception as e:
        print(str(e))
    protocols = pd.read_csv("./csv/CyanviewDescriptor-CameraProtocols.csv",usecols=["Protocol","Brand","Type","Cable","SupportURL","Message","MaxDelayToComplete","ControlCoverage","Bidirectionnal"])
    proto_df = pd.DataFrame(protocols)
    del proto_df['Brand']
    pool_df = pd.merge(cam_df, proto_df, on = ['Protocol'],how = 'left').set_index('Model')
    pool_df.index = pool_df.index.str.upper()
    # Add missing columns
    pool_df = pool_df.assign(Selected=False)
    pool_df = pool_df.assign(Number=0)
    pool_df = pool_df.assign(Lens='Fixed')
    pool_df = pool_df.assign(Network='LAN wired')
    pool_df = pool_df.assign(Base='Fixed')
    ## To suppress ??
    #df['Model'] = df.index
    pool_df.to_csv("./picklized/Generated_CameraDetails.csv")
    return (pool_df)

def picklize_messages():
    message_dic = {}
    def store(topic,subtopic,message):
        if topic not in message_dic : message_dic[topic]={}
        if subtopic not in message_dic[topic]: message_dic[topic][subtopic]={}
        message_dic[topic][subtopic]=message

    p  = re.compile(r"/\[(.*)\,(.*)\]")
    message = ""
    with open('./Messages.md', 'r') as reader:
        line = reader.readline()
        print("Line: ",line)
        first_line = True
        while line != '':  # The EOF char is an empty string
            if line[0:2]== "/[":
                if first_line:
                    # No message to store
                    first_line = False
                else:
                    # Store currently collected message
                    store(topic,subtopic,message)
                    message = ""
                result   = p.search(line)
                topic    = result.group(1)
                subtopic = result.group(2)
            else:
                message += line
                # print("Keys: ",context, state,name)
                # print("Message: ",message)
            line = reader.readline()
        # Store last message
        store(topic,subtopic,message)           
        return (message_dic)

def picklize_options():
    options = {}
    with open('./csv/CyanviewDescriptor-Options.csv', mode='r') as csv_file:
        #csv_reader = csv.reader(csv_file, delimiter=',')
        csv_reader = csv.reader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                baseKeys = row
            elif line_count == 1:
                suffixKeys = row
                keyOrder = []
                columnsNumber = len(row)
                for i in range(columnsNumber):
                    key = baseKeys[i] + suffixKeys[i]
                    options[key]=[]
                    keyOrder.append(key)
            else:
                for i in range(len(row)):
                    if row[i] != "":
                        options[keyOrder[i]].append(row[i])
            line_count += 1
    return(options)
        # print(f'Processed {line_count} lines.')
        # print(self.options)
        # for key in self.options:
            # print("\n\n",key," : ",self.options[key])

def picklize_constraints():
    constraints = {}
    constraints_df = pd.read_csv("./csv/CyanviewDescriptor-Constraints.csv",header = [0,1])
    constraints_df = pd.DataFrame(constraints_df)
    constraints_dict = constraints_df.to_dict()
    for key,dico in constraints_dict.items():
        # print("\nRow Dict: ", dico)
        listFromDict = []
        for index,value in dico.items():
            if not (value != value):
                listFromDict.append(value)
        # print("Row list: ",listFromDict)
        constraints_dict[key] = listFromDict.copy()
    constraints = constraints_dict
    return(constraints)
    # print(list(constraints_dict.keys()))
    # print(constraints_dict[('Slow Motion', 'Network')])
    # print(constraints_dict)


def picklize():
    getGoogleDescriptorSheets()
    df  = picklize_cameras()
    df.to_pickle("./picklized/cameras.pkl")
    messages = picklize_messages()
    with open('./picklized/messages.pkl', 'wb') as file:
        pickle.dump(messages, file)
    options     = picklize_options()
    constraints = picklize_constraints()
    print("Options picklized : \n",options)

    properties = {}
    properties["options"]     = options
    properties["constraints"] = constraints
    with open('./picklized/properties.pkl', 'wb') as file:
        pickle.dump(properties, file)

def getGoogleDescriptorSheets():
    outDir = 'csv/'
    spreadsheet_id = "1_oZTlFz0q8U15xqHXeq9gnG684GpGqWm_6oMOwYeWq4"
    ids = [
    "0", #Options
    "50235224",   # Constraints
    "1339909585", # Cameras
    "2097676387", # CameraProtocols
    ]
    for id in ids:
        url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={id}'
        response = requests.get(url)
        if response.status_code == 200:
            d = response.headers["content-disposition"]
            fname = re.findall(r'filename="(.+)"', d)[0]
            print(f"Downloaded filename: {fname}")
            filepath = os.path.join(outDir, fname)
            with open(filepath, 'wb') as f:
                f.write(response.content)
                print('CSV file saved to: {}'.format(filepath))    
        else:
            print(f'Error downloading Google Sheet: {response.status_code}')
            sys.exit(1)


if __name__ == "__main__":
    picklize()
    print("Picklize done")
    with open('./picklized/properties.pkl', 'rb') as file:
        properties = pickle.load(file)    

    print("Properties Unpicklized:\n",properties)    
    print("Options dictionnary:\n",properties["options"])
    print("Constrainrs dictionnary:\n",properties["constraints"])

    
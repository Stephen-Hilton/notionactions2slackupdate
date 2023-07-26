import requests, os, json, certifi, ssl, time, datetime, pytz, logging
from dotenv import load_dotenv
from pytz import timezone
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logging.basicConfig(filename='./notion2slack.log', encoding='utf-8', level=logging.INFO)
logging.info("Loading variables and configs")

load_dotenv()
notion_api_key = os.getenv('NOTION_API_KEY')
slack_api_key = os.getenv('SLACK_API_KEY')
standard_headers = {"Notion-Version": "2022-06-28"
                   ,"content-type": "application/json"
                   ,"Authorization": f"Bearer {notion_api_key}"}
config = json.load(open('config.json'))
logging.info(f'json content: \n{str(config)}')


def get_result(results_json, parent_list=[]):
    json_position = results_json
    for itm in parent_list:
        if json_position and itm in json_position or (type(itm) == int and itm < len(json_position)): 
            json_position = json_position[itm]
        else:
            return ""
    return json_position if json_position else ""


def get_users(include=['all']):
    resp = requests.get("https://api.notion.com/v1/users", headers=standard_headers)
    users = {}
    if resp.status_code == 200:
        for obj in resp.json()['results']:
            if obj['object'] == 'user' and obj['type'] == 'person' and ('all' in include or obj['name'] in include):
                users[ obj['id'] ] = { 'id': obj['id'], 'name': obj['name'], 'email': obj['person']['email'], 'actions':[] }
    return users 


def get_account(id):
    url = f'https://api.notion.com/v1/pages/{id}'
    resp = requests.get(url=url, headers=standard_headers)
    acct = {'id': id }
    if resp.status_code == 200:
        acct_json = resp.json()
        acct['Name'] = get_result(acct_json, ['properties','Name','title',0,'plain_text'])
        acct['Website'] = get_result(acct_json, ['properties','Website','url'])
        acct['Priority'] = get_result(acct_json, ['properties','Priority','select','name'])
    return acct 


def get_actions(userid, max_items=10) -> json:
    actionsid = get_result(config, ["notion databases","Actions"])
    url = f'https://api.notion.com/v1/databases/{actionsid}/query'
    # ,{ "property": "Priority", "select": { "equals": "0 - Urgent", "equals": "1 - Important" } }
    data_json = {"filter": 
                    { "and": 
                     [   { "property": "Owner", "people": { "contains": userid } }
                        ,{ "property": "Status", "status": { "does_not_equal": "8 - Done"} }
                        ,{ "property": "Status", "status": { "does_not_equal": "9 - Expired" } }
                     ] },
                "sorts": [ { "property": "Priority", "direction": "ascending" },
                           { "property": "Due Date", "direction": "ascending" } ] }
    resp = requests.post(url=url, headers=standard_headers, json=data_json)
    msgs = []
    if resp.status_code != 200:
        logging.error(f"Error: {resp} " )
    else:
        
        i=0
        for action in resp.json()['results']:
            action_title = get_result(action, ['properties','Short Description','title',0,'plain_text'])

            # wrestle with dates:
            action_duedate = get_result(action, ['properties','Due Date','date','end'])
            action_startdate = get_result(action, ['properties','Start Date','date','start'])
            if action_startdate == "" and action_duedate != "": 
                action_startdate = get_result(action, ['properties','Due Date','date','start'])
            if action_duedate == "": 
                action_duedate = get_result(action, ['properties','Due Date','date','start'])
            if action_startdate == "": 
                action_startdate = get_result(action, ['created_time']).split('T')[0]
            action_startdate_dt = datetime.datetime.strptime(action_startdate,'%Y-%m-%d')
            action_duedate_dt = datetime.datetime.strptime(action_duedate,'%Y-%m-%d')
            action_duedate_late = action_duedate_dt < datetime.datetime.now()
            action_startdate_early = action_startdate_dt > datetime.datetime.now()

            # stop here if the action is future-dated, otherwise count and continue
            if action_startdate_early: 
                continue
            else:
                i+=1
                if i >max_items: break 

            # get other attributes:
            
            action_url = get_result(action, ['url'])
            action_status = get_result(action, ['properties','Status','status','name'])[4:]
            action_priority = get_result(action, ['properties','Priority','select','name'])[4:]
            action_account_id = get_result(action, ['properties','Accounts','relation',0,'id'])
            
            # get account information, stored in a different Notion table / API call:
            if action_account_id == '':
                action_account_json     = '' 
                action_account_name     = '' 
                action_account_url      = '' 
                action_account_priority = '' 
                action_account_px       = '' 
            else:
                action_account_json     =  get_account(action_account_id) # go fetch from Notion
                action_account_name     =  action_account_json['Name']
                action_account_url      =  action_account_json['Website']
                action_account_priority =  action_account_json['Priority']
                action_account_px       =  f' (P{action_account_priority[:1]})' if action_account_priority !="" else ""

            # set icon 
            action_icon = '_'.join([':project', \
                                'red' if action_duedate_late else 'green', \
                                action_priority.lower() if action_priority.lower() in['urgent','important'] else 'normal', \
                                'enterprise:' if action_account_priority[:1]=='1' else 'customer:'])

            sep = '' if action_priority=='' and action_account_name=='' else ':'
            sep2 = ', ' if action_status !='' and action_priority !='' else ''
            msgs.append(f'{action_icon} #{i}: {action_title} ({action_priority}{sep2}{action_status})\n{action_account_name}{action_account_px}{sep} due {action_duedate}, start {action_startdate}')
    return msgs     


def slack(channel_id, messages=[]):
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    client = WebClient(token=slack_api_key, ssl=ssl_context)
    try:
        for message in messages:
            response = client.chat_postMessage(channel=channel_id, text=message )
    except SlackApiError as e:
        logging.error(f"Error publishing message: {e.response['error']}")



    
# control:
while True:
    logging.info(f'\nDaily process starting: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}') 

    # Process once per user
    for user in config['users']:    
        logging.info(f"Generating { user['name'] }")
        now_utc = datetime.datetime.now(tz=pytz.utc)
        now = datetime.datetime.strftime( now_utc.astimezone(pytz.timezone('US/Pacific')) , '%Y-%m-%d %H:%M:%S')
        user['actions'] = get_actions( user['notionid'], int(user['items']) )
        actions = [a for a in user['actions'] ]
        actions.insert(0,f'----------------------------------------------------------')
        actions.insert(1,f'\n*TOP NotionCRM ACTION ITEMS for {user["name"]} on {now}')
        actions.insert(2,f'\nSee <https://www.notion.so/{ config["notion databases"]["Actions"] }|NotionCRM Actions> for more detail')
        actions.append('---\n')
        slack(user['slackid'], actions)

    # sleep until 5am
    now_utc = datetime.datetime.now(tz=pytz.utc)
    now = now_utc.astimezone(pytz.timezone('US/Pacific'))
    next_runtime = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=5, minute=0, second=0, tzinfo=now.tzinfo) + datetime.timedelta(days=1)
    seconds_to_next_run = int((next_runtime - now).total_seconds())
    logging.info(f'Complete, sleeping until {next_runtime.strftime("%Y-%m-%d %H:%M:%S")}, which is {seconds_to_next_run} seconds from now.')
    time.sleep(seconds_to_next_run)


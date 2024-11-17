import json
import pandas as pd

def process_person(person_file):
    with open(person_file) as file:
        person_data = {}
        json_str = file.read()
        person_content = json.loads(json_str)['person']
        return pd.Series(person_content)

def process_vote(votes_file):
    with open(votes_file) as file:
        votes_data = {}
        json_str = file.read().replace('"0000-00-00"', 'null')
        
        roll_call_content = json.loads(json_str)['roll_call']
        votes_data = roll_call_content
        votes = roll_call_content['votes']
        for vote in votes:
            if len(votes) > 0:
                for vote in votes:
                    votes_data['people_id'] = vote['people_id']
                    votes_data['vote_id'] = vote['vote_id']
                    votes_data['vote_text'] = vote['vote_text']
                else:
                    votes_data['people_id'] = 'null'
                    votes_data['vote_id'] = 'null'
                    votes_data['vote_text'] = 'null'
           
        return pd.Series(votes_data)


def process_sponsor(bill_file):
    with open(bill_file) as file:
        sponsor_data = {}
        json_str = file.read().replace('"0000-00-00"', 'null')
        
        bill_content = json.loads(json_str)['bill']
    
        sponsors = bill_content['sponsors']
        if len(sponsors) > 0:
            for sponsor in sponsors:
                sponsor_data['people_id'] = sponsor['people_id']
                sponsor_data['bill_id'] = bill_content['bill_id']
        else:
            sponsor_data['bill_id'] = 'null'
            sponsor_data['people_id'] = 'null'
        return pd.Series(sponsor_data)

def process_session(bill_file):
    with open(bill_file) as file:
        session_data = {}
        json_str = file.read().replace('"0000-00-00"', 'null')
        
        bill_content = json.loads(json_str)['bill']
        try:
            session_data = bill_content['session']
        except:
            session_data['session_id'] = 'null'
            session_data['bill_id'] = 'null'
            session_data['state_id'] = 'null'
            session_data['year_start'] = 'null'
            session_data['year_end'] = 'null'
            session_data['prefile'] = 'null'
            session_data['sine_die'] = 'null'
            session_data['prior'] = 'null'
            session_data['special'] = 'null'
            session_data['session_tag'] = 'null'
            session_data['session_title'] = 'null'
            session_data['session_name'] = 'null'
        return pd.Series(session_data)

def process_bill(bill_file):
    with open(bill_file) as file:
        bill_data = {}
        json_str = file.read().replace('"0000-00-00"', 'null')
        content = json.loads(json_str)['bill']

        bill_data['bill_id'] = content['bill_id']
        bill_data['change_hash'] = content['change_hash']
        bill_data['session_id'] = content['session_id']
        bill_data['state_link'] = content['state_link']
        bill_data['completed'] = content['completed']
        bill_data['status'] = content['status']
        bill_data['status_date'] = content['status']
        bill_data['status_date'] = content['status']

        bill_data['bill_number'] = content['bill_number']
        bill_data['title'] = content['title']
        bill_data['description'] = content['description']
        bill_data['state'] = content['state']
        bill_data['filename'] = bill_file
        bill_data['status'] = content['status']
        bill_data['state'] = content['state']
        bill_data['state_id'] = content['state_id']
        bill_data['bill_number'] = content['bill_number']
        bill_data['bill_type'] = content['bill_type']
        bill_data['bill_type_id'] = content['bill_type_id']
        bill_data['body'] = content['body']
        bill_data['body_id'] = content['body_id']
        bill_data['current_body'] = content['current_body']
        bill_data['current_body_id'] = content['current_body_id']
        bill_data['title'] = content['title']
        bill_data['description'] = content['description']
        bill_data['pending_committee_id'] = content['pending_committee_id']

        try:
            bill_data['url'] = content['texts'][-1]['state_link'] # most recent text version
        except:
            bill_data['url'] = 'null'

        return pd.Series(bill_data)
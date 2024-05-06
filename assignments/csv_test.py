import csv

import pymongo.cursor
from pymongo import MongoClient

def query_csv(query_result:pymongo.cursor.Cursor,csv_dir,mode='w'):
    '''
    write query results into a csv_file
    :param query_result:
    :param csv_dir:
    :param mode:
    :return:
    '''
    with open(csv_dir,mode,newline='', encoding='UTF_8') as f:
        field_name = query_result[0].keys() # get heads , assuming that all the entries have same heads

        writer = csv.DictWriter(f, fieldnames=field_name)
        writer.writeheader()
        for document in result:
            writer.writerow(document)



mongodb_uri = 'mongodb://localhost:27017'
database = 'Summer_Practice'
collection_name = 'identity'

client = MongoClient(mongodb_uri)#connect to the database
db = client[database]
collection = db[collection_name] # make an instance collection

query = {"title" : "队长"}
projection = {'name': 1, '_id': 0}

result = collection.find(query,projection) #the same syntax as mongodb
csv_dir = 'query_result.csv'
print(result[0]) # the result is a list of which the entries are dicts

query_csv(result,csv_dir) # write first question
#second question
query = [
    {
        '$match' : {
        "phone" : "99999999999"
        }
    },
    {
        '$lookup' : {
       'from' : "team",
        'localField': "belong",
        'foreignField' :"leader_tel",
        'as' : "test"
        }
    },
    {
    '$unwind': "$test"
    },
    {
        '$project':{
            '_id' : 0,
            "name" : "$test.name"
        }
    }
]
cursor = collection.aggregate(query)
result = list(cursor)

query_csv(result,'result_2.csv')
#question3
collection_name = 'team'
collection = db[collection_name]
query = [
    {
        '$match': {'academy':'计算机院'}
    },
    {
        '$lookup':
            {
                'from':'identity',
                'localField': 'leader_tel',
                'foreignField': 'belong',
                'as' : 'team_members'
            }
    },
    {
        '$unwind' : '$team_members'
    },
    {
        '$project' :
            {
                '_id' : 0,
                'name' : '$team_members.name',
                'phone' : '$team_members.phone',
                'title' : '$team_members.title',
                'team' : '$name'
            }
    }
]

cursor = collection.aggregate(query)

result = list(cursor)
#print(result[0])

query_csv(result,'result_3.csv')


#question4:应该在2023-07-20这一天打卡而没打卡的队员信息
from datetime import datetime
date = datetime(2023,7,20)
collection_name = 'identity'
collection = db[collection_name]
cursor = db.aggregate([
    {'$lookup':{
        'from':'sign',
        'localField':'phone',
        'foreignField':'identity',
        'as':'signed'
    }},
    {
        '$unwind':{'signed':'$signed'}
    },
    {
        '$match':{'$or':[{'signed':{'$exist' : False}},{'signed.create_time':date}]}
    },
    {
        '$project':{'_id':0}
    }


])
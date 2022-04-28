import requests
import psycopg2

# connection establishment
conn = psycopg2.connect(
    database="postgres",
    user='postgres',
    password='postgres',
    host='localhost',
    port= '5432'
)
conn.autocommit = True

cursor = conn.cursor()

#create a new database
sql = ''' CREATE database dummyapi '''

cursor.execute(sql)

conn.close()

#connect new database created
conn = psycopg2.connect(
    database="dummyapi",
    user='postgres',
    password='postgres',
    host='localhost',
    port= '5432'
)

cursor = conn.cursor()

#create users table in dummyapi database
sql = '''CREATE TABLE users (
        id VARCHAR, 
        title VARCHAR, 
        firstName VARCHAR, 
        lastName VARCHAR, 
        picture VARCHAR
    )'''

cursor.execute(sql)

#fetch user data from api and store in json format
r = requests.get("https://dummyapi.io/data/v1/user", headers = {"app-id":"62697928b0a4fe4c58dc1326"})
data = r.json()['data']

#get column names for users table
fields = data[0].keys()

#insert each user data into table
for item in data:
    my_data = [item[field] for field in fields]
    cursor.execute("INSERT INTO users VALUES (%s, %s, %s, %s, %s)", tuple(my_data))

conn.commit()

#get users list from database
sql = '''SELECT id from users'''
cursor.execute(sql)
result = cursor.fetchall()
users = [tup[0] for tup in result]

#create posts table to store posts of each user
sql = '''CREATE TABLE posts (
        userID VARCHAR, 
        postID VARCHAR, 
        image VARCHAR, 
        likes INTEGER, 
        tags VARCHAR[],
        text VARCHAR, 
        publishDate VARCHAR
    )'''

cursor.execute(sql)

#insert post data for each user into table
for id in users:
    
    #api url to fetch post data for a particular user id
    url = "https://dummyapi.io/data/v1/user/"+id+"/post"
    
    #fetch post data from api and store in json format
    r = requests.get(url, headers = {"app-id":"62697928b0a4fe4c58dc1326"})
    data = r.json()['data']
    
    #get column names for posts table
    fields = list(data[0].keys())
    fields.remove('owner')

    #insert posts with current user id into table
    for item in data:
        my_data = [item[field] for field in fields]
        my_data.insert(0, id)
        cursor.execute("INSERT INTO posts VALUES (%s, %s, %s, %s, %s, %s, %s)", tuple(my_data))
        conn.commit()

#close connection
conn.close()
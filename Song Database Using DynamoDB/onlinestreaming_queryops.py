import boto3
from boto3.dynamodb.conditions import Key
import hashlib
import random
from decimal import Decimal
from datetime import date 
from pprint import pprint

#Access Pattern 1
#1.1 How many streams does a particular song have?
# The SK is used to filter out the SONG entity, whose SK is "SONG".

def query_number_of_streams(song):
   dynamodb = boto3.resource('dynamodb')
   table = dynamodb.Table('onlinestreaming')
   response = table.query(
       KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                              Key('SK').begins_with('#')
    )
   return response['Items']

#1.2 How many times has a song been streamed in a particular year and month?

def query_stream_activity(song, stream_year, stream_month):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                               Key('SK').begins_with('#YEAR#{0}#MONTH#{1}'.format(stream_year, stream_month))
    )
    return response['Items']

#1.3 How many times has a song been streamed in a particular year?

def query_stream_activity_year_only(song, stream_year):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                               Key('SK').begins_with('#YEAR#{0}'.format(stream_year))
    )
    return response['Items']

#Access Pattern 2
#2.1 How many times did a user listen to a specific song? 

def query_user_stream(username, song):
   dynamodb = boto3.resource('dynamodb')
   table = dynamodb.Table('onlinestreaming')
   
   response = table.query(
       IndexName='Stream_Index',
       KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                              Key('LSI_SK1').begins_with('#USER#{0}'.format(username))
       )
   return response['Items']

#2.2 How many times did a user stream a specific song in a specific year and month?

def query_user_stream_activity(username, song, stream_year, stream_month):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Stream_Index',
        KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                               Key('LSI_SK1').begins_with('#USER#{0}#YEAR#{1}#MONTH#{2}'.format(username, stream_year, stream_month))
    )
    return response['Items']

#2.3 How many times did a user stream a specific song in a specific year only?

def query_user_stream_activity_year_only(username, song, stream_year):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Stream_Index',
        KeyConditionExpression=Key('PK').eq('#SONG#{0}'.format(song)) &
                               Key('LSI_SK1').begins_with('#USER#{0}#YEAR#{1}'.format(username, stream_year))
    )
    return response['Items']

#Access Pattern 3
#3.1 How active is the streaming service on a particular year and month?
#This can be helpful to the streaming service developers in determining if people are using the service.

def query_online_streaming_activity(stream_year, stream_month):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Stream_Activity',
        KeyConditionExpression=Key('GSI_PK1').eq('#YEAR#{0}#MONTH#{1}'.format(stream_year, stream_month))
    )
    return response['Items']

#Access Pattern 4
#4.1 How many songs does a particular artist have and what are they?
def query_artist_songs(artist_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Artist_Index',
        KeyConditionExpression=Key('GSI_PK2').eq(artist_name) &
                               Key('GSI_SK2').begins_with('#SONG#')
    )

    return response['Items']

#4.2 How many albums does a particular artist have and what are they?
def query_artist_albums(artist_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Artist_Index',
        KeyConditionExpression=Key('GSI_PK2').eq(artist_name) &
                               Key('GSI_SK2').begins_with('#ALBUM#')
    )
    return response['Items']

#Access Pattern 5
#5.1 What songs are in an artist's album?
def query_album_songs(artist_name, album_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Album_Index',
        KeyConditionExpression=Key('GSI_PK3').eq('#ARTIST#{0}#ALBUM#{1}'.format(artist_name, album_name))
    )
    return response['Items']

#Access Pattern 6
#6.1.1 How many songs has a producer produced? What are these songs? 
def query_producer_songs(producer_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Producer_Index',
        KeyConditionExpression=Key('GSI_PK4').eq('#PROD#{0}'.format(producer_name))
    )
    return response['Items']

#Access Pattern 7
#7.1.1 How many songs are under a specific genre? What are these songs? 
def query_genre_songs(song_genre):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    response = table.query(
        IndexName='Genre_Index',
        KeyConditionExpression=Key('GSI_PK5').eq('#GENRE#{0}'.format(song_genre))
    )
    return response['Items']
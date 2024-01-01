import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import hashlib
import random, string

#The following function creates a user. The input should be made by the user.
#This function is used in populating the table.

def create_user(name_of_user):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    username = name_of_user
    user_id = ''.join(random.sample(string.ascii_letters + string.digits, k=5))
    try: 
        user = {
            'PK'      : '#USER#{0}'.format(username), 
            'SK'      : 'USER',
            'User_ID' : user_id
        }
        table.put_item(Item=user, ConditionExpression='attribute_not_exists(PK)')
        print("User {0} created.".format(username))
    except Exception as err:
        print("Error message {0}".format(err))
        print(f"User {username} already exists in database with user ID of {user_id}.")
  
#The following function adds songs to the table.
#This function is used to populate the table.

def add_songs(songsdata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    
    song_id = songsdata['song_id']
    song = songsdata['song']
    artist_name = songsdata['artist_name']
    producer_name = songsdata['producer_name']
    album_name = songsdata['album_name']
    release_year = songsdata['release_year']
    release_month = songsdata['release_month']
    release_day = songsdata['release_day']
    song_genre = songsdata['song_genre']
    duration = songsdata['duration']
    
    item = {
        'PK'                : '#SONG#{0}'.format(song),
        'SK'                : 'SONG',
        'GSI_PK2'           : artist_name,
        'GSI_SK2'           : '#SONG#{0}'.format(song),
        'GSI_PK3'           : '#ARTIST#{0}#ALBUM#{1}'.format(artist_name, album_name),
        'GSI_SK3'           : '#SONG#{0}'.format(song),
        'GSI_PK4'           : '#PROD#{0}'.format(producer_name),
        'GSI_SK4'           : '#YEAR#{0}#SONG#{1}'.format(release_year, song),
        'GSI_PK5'           : '#GENRE#{0}'.format(song_genre),
        'GSI_SK5'           : '#ARTIST#{0}#SONG#{1}'.format(artist_name, song),
        'Song_ID'           : song_id,
        'Artist'            : artist_name,
        'Producer'          : producer_name,
        'Album'             : album_name,
        'Release_Date'      : '#YEAR#{0}#MONTH#{1}#DAY{2}#'.format(release_year, release_month, release_day),
        'Song_Genre'        : song_genre,
        'Duration'          : duration
    }
    
    table.put_item(Item=item)
    print("Added {0} into the online streaming database. The song's ID is {1}.".format(song, song_id))

#The following function adds a song's stream to the table. Its primary key is the title of the song.
#This function is used to populate the table.

def add_streams(streamsdata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    song_id = streamsdata['song_id']
    stream_year = streamsdata['stream_year']
    stream_month = streamsdata['stream_month']
    stream_id = streamsdata['stream_id']
    song = streamsdata['song']
    username = streamsdata['username']
    
    
    item = {
        'PK'                : '#SONG#{0}'.format(song),
        'SK'                : '#YEAR#{0}#MONTH#{1}#ID#{2}'.format(stream_year, stream_month, stream_id),
        'LSI_SK1'           : '#USER#{0}#YEAR#{1}#MONTH#{2}#ID#{3}'.format(username, stream_year, stream_month, stream_id),
        'GSI_PK1'           : '#YEAR#{0}#MONTH#{1}'.format(stream_year, stream_month),
        'GSI_SK1'           : '#SONG#{0}#ID#{1}'.format(song, stream_id),
        'Song_ID'           : song_id,
        'Username'          : username
    }
    table.put_item(Item=item)
    print("A stream of the song titled {0} has been added to the online streaming database with a stream ID of {1}.".format(song, stream_id))

#The following function adds a user to the table. Its primary key is the username.
#This function is used to populate the table.

def add_users(usersdata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    username = usersdata['username']
    user_id = usersdata['user_id']
    
    item = {
        'PK'      : '#USER#{0}'.format(username), 
        'SK'      : 'USER',
        'User_ID' : user_id
    }
    table.put_item(Item=item)
    print("User {0} has been added to the online streaming database with an ID of {1}.".format(username, user_id))

#The following function adds a producer to the table. Its primary key is the producer's name.
#This function is used to populate the table.

def add_prod(proddata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    producer_name = proddata['producer_name']
    producer_id = proddata['producer_id']
    
    item = {
        'PK'                : '#PROD#{0}'.format(producer_name), 
        'SK'                : 'PRODUCER',
        'Producer_ID'       : producer_id
    }
    table.put_item(Item=item)
    print("Producer {0} has been added to the online streaming database with an ID of {1}.".format(producer_name, producer_id))

#The following function adds an artist to the table. Its primary key is the artist's name.
#This function is used to populate the table.

def add_artist(artistdata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    artist_name = artistdata['artist_name']
    artist_id = artistdata['artist_id']
    country = artistdata['country']
    
    item = {
        'PK'                : '#ARTIST#{0}'.format(artist_name), 
        'SK'                : 'ARTIST',
        'Artist_ID'         : artist_id,
        'Country'           : country
    }
    table.put_item(Item=item)
    print("Artist {0} has been added to the online streaming database with an ID of {1}.".format(artist_name, artist_id))

#The following function adds an album to the table. Its primary key is the artist's name.
#This function is used to populate the table.

def add_album(albumdata):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    artist_name = albumdata['artist_name']
    album_name = albumdata['album_name']
    album_id = albumdata['album_id']
    
    item = {
        'PK'                : '#ALBUM#{0}'.format(album_name), 
        'SK'                : 'ALBUM',
        'GSI_PK2'           : artist_name,
        'GSI_SK2'           : '#ALBUM#{0}'.format(album_name),
        'Album_ID'          : album_id,
        'Artist'            : artist_name
    }
    table.put_item(Item=item)
    print("Album {0} by {1} has been added to the online streaming database with an ID of {2}.".format(album_name, artist_name, album_id))
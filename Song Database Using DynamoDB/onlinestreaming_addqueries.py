import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import random
import string
import datetime
import onlinestreaming_tableops as table

def create_artist(artist, artistcountry):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    artist_name = artist
    artist_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    country = artistcountry
    
    try:
      item = {
          'PK'                : '#ARTIST#{0}'.format(artist_name), 
          'SK'                : 'ARTIST',
          'Artist_ID'         : artist_id,
          'Country'           : country
      }
      table.put_item(Item=item, ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists(Country)')
      print("Artist {0} has been created and added to the online streaming database with an ID of {1}.".format(artist_name, artist_id))
    except Exception as err:
        print("Error message {0}".format(err))
        print(f"Artist {artist} with ID of {artist_id} already exists in the database located in {country}.")
  
def create_producer(producer):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('onlinestreaming')
  producer_name = producer
  producer_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
  
  try:
    item = {
        'PK'                : '#PROD#{0}'.format(producer_name), 
        'SK'                : 'PRODUCER',
        'Producer_ID'       : producer_id
    }
    table.put_item(Item=item, ConditionExpression='attribute_not_exists(PK)')
    print("Producer {0} has been created and added to the online streaming database with an ID of {1}.".format(producer_name, producer_id))
  except Exception as err:
      print("Error message {0}".format(err))
      print(f"Producer {producer} with ID of {producer_id} already exists in the database.")
  
def create_album(album_title, artist):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    artist_name = artist
    album_name = album_title
    album_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    
    try:
      item = {
          'PK'                : '#ALBUM#{0}'.format(album_name), 
          'SK'                : 'ALBUM',
          'GSI_PK2'           : artist_name,
          'GSI_SK2'           : '#ALBUM#{0}'.format(album_name),
          'Album_ID'          : album_id,
          'Artist'            : artist_name
      }
      table.put_item(Item=item, ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists (GSI_PK2)')
      print("Album {0} by {1} has been created and added to the online streaming database with an ID of {2}.".format(album_name, artist_name, album_id))
    except Exception as err:
        print("Error message {0}".format(err))
        print(f"The album titled {album_title} by {artist} already exists in the database with an album ID of {album_id}.")

def create_song(song_title, album_title, artist, producer, genre, duration_of_song, year_of_release, month_of_release, day_of_release):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    song_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    song = song_title
    artist_name = artist
    producer_name = producer
    album_name = album_title
    release_year = year_of_release
    release_month = month_of_release.upper()
    release_day = day_of_release
    song_genre = genre
    duration = str(duration_of_song)

    try:
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
      table.put_item(Item=item, ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists (GSI_PK2) AND attribute_not_exists(Album)')
      print("Created and added {0} into the online streaming database. The song's ID is {1}.".format(song, song_id))
    except Exception as err:
        print("Error message {0}".format(err))
        print(f"The song titled {song_title} in the album {album_title} by {artist} already exists in the database with a song ID of {song_id}.")

def create_stream(song_title, id_of_song, user):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('onlinestreaming')
    stream_id_part_1 = "SO"
    stream_id_part_2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 16))
    stream_id = stream_id_part_1 + stream_id_part_2
    streamdate = datetime.datetime.now()
    stream_year = datetime.datetime.now().year
    stream_month = streamdate.strftime("%B").upper()
    song = song_title
    song_id = id_of_song
    username = user
    
    try: 
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
        print("A stream of the song titled {0} has been created and added to the online streaming database with a stream ID of {1} on {2} {3}.".format(song, stream_id, stream_month, stream_year))
    except Exception as err:
        print("Error message {0}".format(err))

def creation():
    print("Good day, developer! Welcome to our online streaming database. Listed below are the actions that one can perform.")
    print("A. Create user.")
    print("B. Create artist.")
    print("C. Create producer.")
    print("D. Create album.")
    print("E. Create song.")
    print("F. Add streams.")
    devaction = str(input("What action would you like to perform? Please input the corresponding letter here: ")).upper()
    
    if devaction == "A":
        if __name__ == '__main__':
            name_of_user = str(input("Please input the unique username: "))
            table.create_user(name_of_user)
    
    elif devaction == "B":
        if __name__ == '__main__':
            name_of_artist = str(input("Please input the artist's name: "))
            country = str(input("Please input the country of the artist: "))
            create_artist(name_of_artist, country)
        
    elif devaction == "C":
        if __name__ == '__main__':
            name_of_prod = str(input("Please input the producer's name: "))
            create_producer(name_of_prod)
    
    elif devaction == "D":
        if __name__ == '__main__':
            name_of_album = str(input("Please input the title of the album: "))
            name_of_artist = str(input("Please input the name of the artist of the album: "))
            create_album(name_of_album, name_of_artist)
    
    elif devaction == "E":
        if __name__ == "__main__":
            song = str(input("Please input the title of the song: "))
            name_of_album = str(input("Please input the title of the album: "))
            name_of_artist = str(input("Please input the name of the artist of the album: "))
            name_of_prod = str(input("Please input the producer's name: "))
            song_genre = str(input("Please input the song's genre: "))
            song_duration = str(input("Please input the duration of the song: "))
            release_year = str(input("Please input the song's year of release: "))
            release_month = str(input("Please input the song's month of release: "))
            release_day = str(input("Please input the song's day of release: "))
            create_song(song, name_of_album, name_of_artist, name_of_prod, song_genre, song_duration, release_year, release_month, release_day)
            
    elif devaction == "F":
        if __name__ == "__main__":
            name_of_song = str(input("Please input the title of the streamed song: "))
            song_id = str(input("Please input the ID of the streamed song: "))
            streamed_user = str(input("Please input the name of the user who streamed the song: "))
            create_stream(name_of_song, song_id, streamed_user)

    else:
          print("Invalid input. Please try again.")
          creation()

creation()

def restart():
    devanswer = str(input("Would you like to perform another query? Please input Yes or No: "))
    while devanswer == "Yes":
        creation()
        devanswer = str(input("Would you like to perform another query? Please input Yes or No: "))
    else:
        print("Thank you for using our online streaming database.")

restart()
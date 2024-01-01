import onlinestreaming_tableops as table
import onlinestreaming_queryops as query   

def start_up():
  print("Good day, user! Welcome to our online streaming database. Listed below are the actions that one can perform.")
  print("A. Find out how many streams a particular song has.")
  print("B. Find out how many times a song has been streamed in a particular year and month.")
  print("C. Find out how many times a song has been streamed in a particular year only.")
  print("D. Find out how many times a user listened to a specific song.")
  print("E. Find out how many times a user streamed a specific song in a specific year and month.")
  print("F. Find out how many times a user streamed a specific song in a specific year.")
  print("G. Find out how active the streaming service is in a particular year and month.")
  print("H. Find out how many songs a specific artist has and learn what those songs are.")
  print("I. Find out how many albums a specific artist has and learn what those songs are.")
  print("J. Find out what songs are in an artist's album.")
  print("K. Find out how many songs a particular producer has produced and learn what those songs are.")
  print("L. Find out how many songs are under a particular genre and learn what those songs are.")
  action = str(input("What action would you like to perform? Please input the corresponding letter here: ")).upper() 
  
  #Sample query 1.1
  if action == "A":
        if __name__ == '__main__':
            count = 0
            song_title = str(input("Please input the title of the song: "))
            streams_number = query.query_number_of_streams(song_title)
            for item in streams_number:
                count += 1
            print(f"The song {song_title} has been streamed {count} time/s.")
  
  #Sample query for 1.2
  elif action == "B":
        if __name__ == '__main__':
            count = 0
            song_title = str(input("Please input the title of the song: "))
            stream_year_one = str(input("Please input the year: "))
            stream_month_one = str(input("Please input the month: ")).upper()
            stream_activity = query.query_stream_activity(song_title, stream_year_one, stream_month_one)
            for item in stream_activity:
                count += 1
            print(f"The song {song_title} had {count} stream/s on {stream_month_one} {stream_year_one}.")

  #Sample query for 1.3
  elif action == "C":
        if __name__ == '__main__':
            count = 0
            song_title = str(input("Please input the title of the song: "))
            stream_year_one = str(input("Please input the year: "))
            stream_activity_year = query.query_stream_activity_year_only(song_title, stream_year_one)
            for item in stream_activity_year:
                count += 1
            print(f"The song {song_title} had {count} stream/s in {stream_year_one}.")
    
  #Sample query 2.1
  elif action == "D":
        if __name__ == '__main__':
            count = 0
            name_of_user = str(input("Please input the user's name: "))
            song_title =  str(input("Please input the title of the song: "))
            user_stream = query.query_user_stream(name_of_user, song_title)
            for item in user_stream:
                count += 1
            print(f"The song {song_title} has been streamed {count} time/s by user {name_of_user}.")
    
  #Sample query 2.2
  elif action == "E":
        if __name__ == '__main__':
            count = 0
            name_of_user = str(input("Please input the user's name: "))
            song_title = str(input("Please input the title of the song: "))
            stream_year_two = str(input("Please input the year: "))
            stream_month_two = str(input("Please input the month: ")).upper()
            streams_of_user = query.query_user_stream_activity(name_of_user, song_title, stream_year_two, stream_month_two)
            for item in streams_of_user:
                count += 1
            print(f"The song {song_title} has been streamed {count} time/s on {stream_month_two} {stream_year_two} by user {name_of_user}.")
    
  #Sample query 2.3
  elif action == "F":
        if __name__ == '__main__':
            count = 0
            name_of_user = str(input("Please input the user's name: "))
            song_title = str(input("Please input the title of the song: "))
            stream_year_two = str(input("Please input the year: "))
            streams_of_user = query.query_user_stream_activity_year_only(name_of_user, song_title, stream_year_two)
            for item in streams_of_user:
                count += 1
            print(f"The song {song_title} has been streamed {count} time/s on {stream_year_two} by user {name_of_user}.")
    
  #Sample query 3.1
  elif action == "G":
        if __name__ == '__main__':
            count = 0
            stream_year_three = str(input("Please input the year: "))
            stream_month_three = str(input("Please input the month: ")).upper()
            online_streams = query.query_online_streaming_activity(stream_year_three, stream_month_three)
            for item in online_streams:
                count += 1
            print(f"For {stream_month_three} {stream_year_three}, there is a total of {count} streams.")
    
  #Sampe query for 4.1
  elif action == "H":
        if __name__ == '__main__':
            count = 0
            name_of_artist = str(input("Please input the artist's name: "))
            number_songs = query.query_artist_songs(name_of_artist)
            for item in number_songs:
                count += 1
            print(f"{name_of_artist} has a total of {count} songs and they have the following songs:")
            for item in number_songs:
                artsong = item['PK'][6:]
                print(artsong)
    
  #Sample query for 4.2
  elif action == "I":
        if __name__ == '__main__':
            count = 0
            album_list = []
            name_of_artist = str(input("Please input the artist's name: "))
            number_albums = query.query_artist_albums(name_of_artist)
            for item in number_albums:
                count += 1
            print(f"{name_of_artist} has a total of {count} album/s and they have the following album/s:")
            for item in number_albums:
                artalbum = item['GSI_SK2'][7:]
                print(artalbum)

  #Sample query for 5.1
  elif action == "J":
        if __name__ == '__main__':
            count = 0
            name_of_artist = str(input("Please input the artist's name: "))
            name_of_album = str(input("Please input the title of the album: "))
            songs_in_albums = query.query_album_songs(name_of_artist, name_of_album)
            for item in songs_in_albums:
                count += 1
            print(f"{name_of_album} by {name_of_artist} has a total of {count} songs and these are the following song/s:")
            for item in songs_in_albums:
                artalbum = item['PK'][6:]
                print(artalbum)
    
  #Sample query for 6.1
  elif action == "K":
        if __name__ == '__main__':
            count = 0
            name_of_producer = str(input("Please input the producer's name: "))
            number_songs = query.query_producer_songs(name_of_producer)
            for item in number_songs:
                count += 1
            print(f"{name_of_producer} has produced a total of {count} songs and these are the following songs:")
            for item in number_songs:
                artsong = item['PK'][6:]
                print(artsong)
    
  #Sample query for 7.1
  elif action == "L": 
        if __name__ == '__main__':
            count = 0
            genre_title = str(input("Please input the genre: "))
            number_songs = query.query_genre_songs(genre_title)
            for item in number_songs:
                count += 1
            print(f"There are a total of {count} songs under the genre {genre_title} and these are the following songs:")
            for item in number_songs:
                artsong = item['PK'][6:]
                print(artsong)
  
  else:
      print("Invalid input. Please try again.")
      start_up()

start_up()

def restart():
    answer = str(input("Would you like to perform another query? Please input Yes or No: "))
    while answer == "Yes":
        start_up()
        answer = str(input("Would you like to perform another query? Please input Yes or No: "))
    else:
        print("Thank you for using our online streaming database.")

restart()
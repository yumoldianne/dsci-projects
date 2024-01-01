import onlinestreaming_tableops as tableops
import csv
from decimal import Decimal
from pprint import pprint

if __name__ == '__main__' :
#for the streams data sheet:
    streamsresp = input('Would you like to add streams in your table? Enter Yes or No: ')
    if streamsresp == 'Yes':
        streamsfile = input('Please enter the streamsdata CSV file name: ')
        streamslist = []
        with open(streamsfile, 'r') as streams:
            for line in csv.DictReader(streams):
                line['song_id'] = str(line['song_id'])
                line['stream_year'] = str(line['stream_year'])
                line['stream_month'] = str(line['stream_month'])
                line['stream_id'] = str(line['stream_id'])
                line['song'] = str(line['song'])
                line['username'] = str(line['username'])
                streamslist.append(line)
        for stream in streamslist:
            tableops.add_streams(stream)
    elif streamsresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')
    
if __name__ == '__main__' :
#for the songs data sheet:
    songsresp = input('Would you like to add songs in your table? Enter Yes or No: ')
    if songsresp == 'Yes':
        songsfile = input('Please enter the songsdata CSV file name: ')
        songslist = []
        with open(songsfile, 'r') as songs:
            for line in csv.DictReader(songs):
                line['song_id'] = str(line['song_id'])
                line['song'] = str(line['song'])
                line['artist_name'] = str(line['artist_name'])
                line['producer_name'] = str(line['producer_name'])
                line['album_name'] = str(line['album_name'])
                line['release_year'] = str(line['release_year'])
                line['release_month'] = str(line['release_month'])
                line['release_day'] = str(line['release_day'])
                line['song_genre'] = str(line['song_genre'])
                line['duration'] = Decimal(str(line['duration']))
                songslist.append(line)
        for song in songslist:
            tableops.add_songs(song)
    elif songsresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')

if __name__ == '__main__' :
#for the users data sheet:
    usersresp = input('Would you like to add users in your table? Enter Yes or No: ')
    if usersresp == 'Yes':
        usersfile = input('Please enter the usersdata CSV file name: ')
        userslist = []
        with open(usersfile, 'r') as users:
            for line in csv.DictReader(users):
                line['user_id'] = str(line['user_id'])
                line['username'] = str(line['username'])
                userslist.append(line)
        for user in userslist:
            tableops.add_users(user)
    elif usersresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')

if __name__ == '__main__' :
#for the producers data sheet:
    prodresp = input('Would you like to add producers in your table? Enter Yes or No: ')
    if prodresp == 'Yes':
        prodfile = input('Please enter the proddata CSV file name: ')
        prodlist = []
        with open(prodfile, 'r') as prod:
            for line in csv.DictReader(prod):
                line['producer_id'] = str(line['producer_id'])
                line['producer_name'] = str(line['producer_name'])
                prodlist.append(line)
        for prod in prodlist:
            tableops.add_prod(prod)
    elif prodresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')

if __name__ == '__main__' :
#for the artists data sheet:
    artistresp = input('Would you like to add artists in your table? Enter Yes or No: ')
    if artistresp == 'Yes':
        artistfile = input('Please enter the artistdata CSV file name: ')
        artistlist = []
        with open(artistfile, 'r') as artist:
            for line in csv.DictReader(artist):
                line['artist_id'] = str(line['artist_id'])
                line['artist_name'] = str(line['artist_name'])
                line['country'] = str(line['country'])
                artistlist.append(line)
        for artist in artistlist:
            tableops.add_artist(artist)
    elif artistresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')

if __name__ == '__main__' :
#for the albums data sheet:
    albumresp = input('Would you like to add albums in your table? Enter Yes or No: ')
    if albumresp == 'Yes':
        albumfile = input('Please enter the albumdata CSV file name: ')
        albumlist = []
        with open(albumfile, 'r') as album:
            for line in csv.DictReader(album):
                line['album_id'] = str(line['album_id'])
                line['album_name'] = str(line['album_name'])
                line['artist_name'] = str(line['artist_name'])
                albumlist.append(line)
        for album in albumlist:
            tableops.add_album(album)
    elif albumresp == 'No':
        print('Okay.')
    else:
        print('Invalid input.')
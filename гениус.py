import requests
import pandas as pd
import time
import random
import re

class GeniusParser:
    def __init__(self, client_id, token):
        self.data = []
        self.client_id = client_id
        self.token = token
        self.session = requests.Session()
        self.setup()

    def setup(self):
        self.session.headers.update({
            'Authorization': f'Bearer {self.token}',
            'X-Genius-Client-ID': self.client_id,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'})

    def get_songs(self, artist, per_page=20, max_pages=50):
        songs = []
        for page in range(1, max_pages + 1):
            try:
                url = "https://api.genius.com/search"
                params = {'q': artist,
                    'page': page,
                    'per_page': per_page}
                resp = self.session.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    hits = data['response']['hits']
                    found = []
                    for hit in hits:
                        song = hit['result']
                        song_artist = song.get('primary_artist', {}).get('name', '').lower()
                        if artist.lower() in song_artist or song_artist in artist.lower():
                            found.append(song)
                    songs.extend(found)
                    if len(hits) < per_page:
                        break
                elif resp.status_code == 429:
                    time.sleep(30)
                    continue
                else:
                    break
                time.sleep(0.5 + random.random())
            except Exception:
                break
        return songs

    def get_artists(self):
        return ['Bruno Mars', 'The Weeknd', 'Calvin Harris', 'Lady Gaga',
                'Rihanna', 'Bad Bunny', 'Taylor Swift', 'David Guetta',
                'Kanye West', 'Drake', 'EJAE', 'Adele', 'Beyoncé', 'Tate McRae',
                'HUNTR/X', 'Michael Jackson', 'REI AMI', 'Pitbull', 'Sam Smith',
                'Benson Boone', 'Tyler, The Creator', 'Linkin Park', 'Miley Cyrus',
                'Shreya Ghoshal', 'Halsey', 'Shawn Mendes', 'Arijit Singh',
                'OneRepublic', 'Olivia Rodrigo', 'The Neighbourhood',
                'Playboi Carti', 'Harry Styles', 'Camila Cabello', 'USHER',
                'Elton John', 'Britney Spears', 'One Direction', 'JENNIE',
                'Selena Gomez', 'Hozier', 'Metro Boomin', 'Ellie Goulding', 'RAYE',
                '50 Cent', 'Red Hot Chili Peppers', '21 Savage', 'Farruko',
                'DJ Snake', 'Peso Pluma', 'The Kid LAROI', 'Olivia Dean', 'Jazeek',
                'Fuerza Regida', 'Kesha', 'Grupo Frontera', 'Daniel Caesar',
                'Flo Rida', 'Beéle', 'The Marías', 'Feid', 'Anuel AA', 'ABBA',
                'Myke Towers', 'Sachin-Jigar', 'Avicii', 'Diplo', 'Tiësto',
                'Alicia Keys', 'Nirvana', 'Swae Lee', 'AC/DC', 'Lord Huron',
                "Guns N' Roses", 'Cigarettes After Sex', 'James Arthur',
                'Amitabh Bhattacharya', 'Morgan Wallen', 'ZAYN', 'Bebe Rexha',
                'The Beatles', 'Lola Young', 'Tinashe', 'Billy Joel',
                'Enrique Iglesias', 'Lewis Capaldi', 'Gunna', 'Empire Of The Sun',
                'Vishal-Shekhar', 'Paramore', 'Anne-Marie', 'Bon Jovi',
                'Twenty One Pilots', 'Saja Boys', 'Metallica', 'Young Thug',
                'Cris MJ', 'PARTYNEXTDOOR', 'Demi Lovato', 'Zara Larsson',
                'samUIL Lee', 'Danny Chung', 'Neckwav', 'KEVIN WOO', 'KK', 'd4vd',
                'Juice WRLD', 'Creedence Clearwater Revival', 'The Cranberries',
                'Snoop Dogg', 'Oasis', 'Carín León', 'Sonu Nigam', 'Neton Vega',
                'Mithoon', 'Kehlani', 'Tyla', 'Childish Gambino', 'BLACKPINK',
                'Atif Aslam', 'Junior H', 'Jennifer Lopez', 'Miguel',
                'The Rolling Stones', 'Clean Bandit', 'Swedish House Mafia',
                'Chinmayi', 'Avril Lavigne', 'Alka Yagnik', 'Shilpa Rao',
                'Lil Uzi Vert', 'Arcángel', 'Joji', 'Trippie Redd', 'Mark Ronson',
                'Kid Cudi', 'Danny Ocean', 'Kodak Black', 'GIVĒON', 'F1 The Album',
                'Bryan Adams', 'Alan Walker', 'Ed Sheeran', 'Mac Miller',
                'Coldplay', 'Justin Bieber', 'Billie Eilish', 'Ariana Grande',
                'Sabrina Carpenter', 'Eminem', 'Kendrick Lamar', 'SZA', 'Shakira',
                'Doja Cat', 'Maroon 5', 'Katy Perry', 'Dua Lipa', 'Post Malone',
                'sombr', 'Chris Brown', 'Travis Scott', 'Arctic Monkeys',
                'Imagine Dragons', 'Marshmello', 'Rauw Alejandro', 'Alex Warren',
                'Khalid', 'Queen', 'Future', 'Ozuna', 'Justin Timberlake',
                'Charlie Puth', 'Teddy Swims', 'The Chainsmokers', 'Radiohead',
                'Maluma', 'Ne-Yo', 'Lil Wayne', 'Nicki Minaj', 'A.R. Rahman',
                'Gorillaz', 'Don Omar', 'Chappell Roan', 'Tame Impala', 'Madonna',
                'The Goo Goo Dolls', 'Akon', 'Mariah Carey', 'Wiz Khalifa',
                'The Police', 'Green Day', 'A$AP Rocky', 'Anitta', 'Ty Dolla $ign',
                'Gracie Abrams', 'Kali Uchis', 'Frank Ocean', 'Manuel Turizo',
                'XXXTENTACION', 'Romeo Santos', 'Irshad Kamil', 'P!nk',
                'Tom Odell', 'KATSEYE', 'Pharrell Williams', 'Charli xcx',
                'J. Cole', 'Nelly Furtado', 'Jason Derulo', 'Ava Max', 'Lorde',
                'Lil Baby', 'Udit Narayan', 'Ravyn Lenae', 'Christina Aguilera',
                'Quevedo', 'Lily-Rose Depp', 'Whitney Houston', 'JHAYCO',
                'Florence + The Machine', 'Daft Punk', 'The Killers', 'DaBaby',
                'Luke Combs', 'De La Soul', 'French Montana', 'Zach Bryan',
                'The Script', 'Vance Joy', 'T-Pain', 'Steve Lacy',
                'Backstreet Boys', 'Gigi Perez', 'TWICE', 'Keane', 'Conan Gray',
                'Becky G', 'Rels B', 'Phil Collins', 'beabadoobee', 'Aerosmith',
                'Tyga', 'Shankar-Ehsaan-Loy']

    def process_song(self, song, artist):
        try:
            title = song.get('title', '').strip()
            song_artist = song.get('primary_artist', {}).get('name', '').strip()
            url = song.get('url', '')
            if not title or not song_artist:
                return None
            date = song.get('release_date', '')
            stats = song.get('stats', {})
            views = stats.get('pageviews', 0)
            album_data = song.get('album', {})
            album = album_data.get('name', 'Single') if album_data else 'Single'
            feat = []
            if 'featured_artists' in song and song['featured_artists']:
                feat = [a.get('name', '') for a in song['featured_artists']]
            year = None
            if date:
                match = re.search(r'(19|20)\d{2}', date)
                if match:
                    year = int(match.group())
            return {'title': title,
                'artist': song_artist,
                'search_artist': artist,
                'feat': ', '.join(feat) if feat else '',
                'date': date,
                'year': year,
                'album': album,
                'url': url,
                'views': views,
                'genre': self.get_genre(song_artist, title),
                'explicit': song.get('explicit', False)}
        except Exception as e:
            return None

    def get_genre(self, artist, title):
        artist_low = artist.lower()
        title_low = title.lower()
        genres = {
            'Rock': ['rock', 'metal', 'punk', 'grunge', 'guitar', 'band', 'linkin', 'nirvana', 'ac/dc', 'guns n\' roses', 'metallica', 'bon jovi'],
            'Pop': ['pop', 'dance', 'disco', 'bubblegum', 'taylor swift', 'katy perry', 'lady gaga', 'rihanna', 'britney'],
            'Hip-Hop': ['rap', 'hip-hop', 'drill', 'trap', 'lil', 'young', 'drake', 'kanye', 'eminem', 'snoop', '50 cent'],
            'R&B': ['r&b', 'soul', 'motown', 'neo soul', 'usher', 'beyoncé', 'alicia keys'],
            'Country': ['country', 'folk', 'americana', 'bluegrass', 'morgan wallen'],
            'Electronic': ['electronic', 'edm', 'house', 'techno', 'dubstep', 'david guetta', 'calvin harris', 'avicii', 'tiësto', 'swedish house mafia'],
            'Indie': ['indie', 'alternative', 'lo-fi', 'bedroom', 'lana del rey', 'hozier', 'the neighbourhood'],
            'Jazz': ['jazz', 'blues', 'swing', 'bebop'],
            'Latin': ['latin', 'reggaeton', 'salsa', 'bachata', 'bad bunny', 'j balvin', 'ozuna'],
            'K-Pop': ['k-pop', 'bts', 'blackpink', 'jennie'],
            'Bollywood': ['bollywood', 'arjit', 'shreya', 'pritam', 'sonu nigam', 'atif aslam']}
        for genre, words in genres.items():
            if any(word in artist_low or word in title_low for word in words):
                return genre
        return 'Popular'

    def main(self):
        artists = self.get_artists()
        songs = []
        for i, artist in enumerate(artists):
            found = self.get_songs(artist, per_page=20, max_pages=8)
            for song in found:
                data = self.process_song(song, artist)
                if data:
                    songs.append(data)
            if i < len(artists) - 1:
                time.sleep(random.uniform(2, 4))
        self.save(songs)

    def save(self, songs):
        if songs:
            df = pd.DataFrame(songs)
            df = df.drop_duplicates(subset=['url'])
            df.to_csv('гениус.csv', index=False, encoding='utf-8')

CLIENT_ID = "UO2Y1G_dpaw8L5tJA0Hvsxt95N85zEziNAgzn1e6QxY2Cao5Ikg5X288l3SYtMVu"
TOKEN = "0bP2r-cw1cd5wL11VvP9LKsp9x6qAmUSsibWhUjAS7BZH-elAeKaIvtbXtQdigl4"

parser = GeniusParser(CLIENT_ID, TOKEN)
parser.main()
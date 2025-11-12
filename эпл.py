import requests
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class MusicCollector:
    def __init__(self):
        self.seen_ids = set()
        self.tracks = []
        self.session = requests.Session()
        self.setup_session()
        self.last_request = datetime.now()
        self.min_interval = 2.0
        self.errors = 0
        self.request_count = 0
        self.good_artists = 0

    def setup_session(self):
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*'})

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

    def make_request(self, url, params, retries=3):
        now = datetime.now()
        time_passed = (now - self.last_request).total_seconds()
        if time_passed < self.min_interval:
            sleep_time = self.min_interval - time_passed
            time.sleep(sleep_time)
        for try_num in range(retries):
            try:
                self.request_count += 1
                response = self.session.get(url, params=params, timeout=30)
                self.last_request = datetime.now()
                if response.status_code == 200:
                    self.errors = 0
                    return response
                elif response.status_code == 429:
                    self.errors += 1
                    wait_time = 60 + (try_num * 30)
                    time.sleep(wait_time)
                    continue
                elif response.status_code >= 500:
                    self.errors += 1
                    time.sleep(10)
                    continue
                else:
                    return None
            except Exception as e:
                self.errors += 1
                if try_num < retries - 1:
                    time.sleep(10)
        return None

    def search_special(self, artist, type, limit=20):
        queries = {'live': f'{artist} live',
                   'remix': f'{artist} remix',
                   'acoustic': f'{artist} acoustic'}
        if type not in queries:
            return []
        params = {'term': queries[type],
                  'media': 'music',
                  'entity': 'song',
                  'limit': limit,
                  'attribute': 'artistTerm'}
        response = self.make_request("https://itunes.apple.com/search", params)
        if not response:
            return []
        data = response.json()
        tracks = data.get('results', [])
        return tracks

    def get_type(self, title, album):
        title_low = title.lower()
        album_low = album.lower()
        if 'live' in title_low or 'live' in album_low:
            return 'live'
        elif 'remix' in title_low:
            return 'remix'
        elif 'acoustic' in title_low:
            return 'acoustic'
        elif 'cover' in title_low:
            return 'cover'
        elif 'session' in title_low or 'session' in album_low:
            return 'session'
        elif 'single' in album_low:
            return 'single'
        else:
            return 'standard'

    def search_artist(self, artist):
        all_tracks = []
        base_tracks = self._search_pages(artist, limit=100)
        all_tracks.extend(base_tracks)
        if len(base_tracks) > 3:
            album_tracks = self._search_albums(artist, limit=100)
            all_tracks.extend(album_tracks)

        if len(base_tracks) > 3:
            extra_tracks = self._search_extra(artist, limit=100)
            all_tracks.extend(extra_tracks)
        unique_tracks = self.remove_dupes(all_tracks)
        return unique_tracks

    def _search_extra(self, artist, limit=30):
        all_tracks = []
        types = ['live', 'remix', 'acoustic']
        for type in types:
            tracks = self.search_special(artist, type, limit=10)
            filtered = [t for t in tracks if t.get('artistName', '').lower() == artist.lower()]
            all_tracks.extend(filtered)
            time.sleep(1)
        return all_tracks[:limit]

    def _search_pages(self, artist, limit=100):
        all_tracks = []
        offset = 0
        for page in range(20):
            if self.errors >= 3:
                time.sleep(30)
                self.errors = 0
            params = {
                'term': artist,
                'media': 'music',
                'entity': 'song',
                'limit': min(50, limit - len(all_tracks)),
                'offset': offset,
                'attribute': 'artistTerm'}
            response = self.make_request("https://itunes.apple.com/search", params, retries=2)
            if not response:
                break
            data = response.json()
            tracks = data.get('results', [])
            if not tracks:
                break
            filtered = [t for t in tracks if t.get('artistName', '').lower() == artist.lower()]
            all_tracks.extend(filtered)
            offset += len(tracks)
            time.sleep(1)
            if len(tracks) < params['limit'] or len(all_tracks) >= limit:
                break
        return all_tracks

    def _search_albums(self, artist, limit=50):
        params = {
            'term': artist,
            'entity': 'album',
            'limit': 50}
        response = self.make_request("https://itunes.apple.com/search", params)
        if not response:
            return []
        data = response.json()
        albums = data.get('results', [])
        all_tracks = []
        for i, album in enumerate(albums[:30]):
            album_id = album.get('collectionId')
            if not album_id:
                continue
            track_params = {'id': album_id,
                            'entity': 'song',
                            'limit': 100}
            track_response = self.make_request("https://itunes.apple.com/lookup", track_params)
            if track_response:
                track_data = track_response.json()
                tracks = track_data.get('results', [])
                album_tracks = tracks[1:] if len(tracks) > 1 else []
                filtered = [t for t in album_tracks if t.get('artistName', '').lower() == artist.lower()]
                all_tracks.extend(filtered)
            time.sleep(1)
        return all_tracks[:limit]

    def remove_dupes(self, tracks):
        seen = set()
        unique = []
        for track in tracks:
            track_id = track.get('trackId')
            if track_id and track_id not in seen:
                seen.add(track_id)
                unique.append(track)
        return unique

    def process_track(self, track, artist):
        if not track.get('trackId'):
            return None
        track_id = track['trackId']
        if track_id in self.seen_ids:
            return None
        title = track.get('trackName', '').strip()
        track_artist = track.get('artistName', '').strip()
        if not title or title == 'Unknown' or len(title) < 2:
            return None
        if track_artist.lower() != artist.lower():
            return None
        data = {'track_id': track_id,
                'title': title,
                'artist': track_artist,
                'album': track.get('collectionName', 'Unknown').strip(),
                'duration_ms': track.get('trackTimeMillis', 0),
                'genre': track.get('primaryGenreName', 'Unknown'),
                'release_date': self.clean_date(track.get('releaseDate')),
                'explicit': track.get('trackExplicitness') == 'explicit',
                'track_number': track.get('trackNumber', 0),
                'url': track.get('trackViewUrl', ''),
                'preview_url': track.get('previewUrl', ''),
                'price': track.get('trackPrice', 0),
                'target_artist': artist,
                'collection_id': track.get('collectionId'), }
        data.update({
            'duration_minutes': round(data['duration_ms'] / 60000, 2) if data['duration_ms'] else 0,
            'release_year': self.get_year(data['release_date']),
            'is_single': data['track_number'] == 1,
            'has_feat_in_title': self.check_feat(data['title']),
            'title_length': len(data['title']),
            'title_word_count': len(data['title'].split()),
            'is_live': 'live' in data['title'].lower(),
            'is_remix': 'remix' in data['title'].lower(),
            'is_acoustic': 'acoustic' in data['title'].lower(),
            'is_cover': 'cover' in data['title'].lower(),
            'content_type': self.get_type(data['title'], data['album'])})
        self.seen_ids.add(track_id)
        return data

    def clean_date(self, date_str):
        if not date_str or len(date_str) < 10:
            return 'Unknown'
        return date_str[:10]

    def get_year(self, date_str):
        if date_str and len(date_str) >= 4 and date_str != 'Unknown':
            return date_str[:4]
        return 'Unknown'

    def check_feat(self, title):
        keywords = ['feat', 'ft.', 'featuring', 'with', '&', 'x ']
        title_low = title.lower()
        return int(any(word in title_low for word in keywords))

    def process_artist(self, artist):
        try:
            tracks = self.search_artist(artist)
            processed = []
            for track in tracks:
                processed_track = self.process_track(track, artist)
                if processed_track:
                    processed.append(processed_track)
            return processed
        except Exception as e:
            return []

    def main(self, batch_size=6, workers=3):
        artists = self.get_artists()
        total = len(artists)
        batches = (total + batch_size - 1) // batch_size
        for batch_num in range(batches):
            start = batch_num * batch_size
            end = min((batch_num + 1) * batch_size, total)
            batch_artists = artists[start:end]
            batch_tracks = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self.process_artist, artist): artist
                    for artist in batch_artists}
                done = 0
                for future in as_completed(futures):
                    try:
                        tracks = future.result()
                        batch_tracks.extend(tracks)
                        done += 1
                        if len(tracks) > 0:
                            self.good_artists += 1
                    except Exception as e:
                        done += 1
            self.tracks.extend(batch_tracks)
            if batch_num < batches - 1:
                time.sleep(30)
        return self.tracks

    def save(self):
        if not self.tracks:
            return None
        df = pd.DataFrame(self.tracks)
        df = df.sort_values(['target_artist', 'release_year'], ascending=[True, False])
        df.to_csv("эпл.csv", index=False, encoding='utf-8')
        return df


if __name__ == "__main__":
    collector = MusicCollector()
    df = collector.main(batch_size=6, workers=4)
    df = collector.save()
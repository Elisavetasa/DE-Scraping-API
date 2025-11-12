import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re


class MusicParser:
    def __init__(self):
        self.url = "https://musicbrainz.org"
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xml;q=0.9,*/*;q=0.8',})
        self.seen = set()

    def get_page(self, url, params=None, tries=3):
        for i in range(tries):
            try:
                resp = self.session.get(url, params=params, timeout=20)
                if resp.status_code == 429:
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i < tries - 1:
                    time.sleep(5)
        return None

    def find_artist(self, name):
        names = [name]
        if name.lower().startswith('the '):
            names.append(name[4:].strip())
        for n in names:
            artist_id, info = self.get_artist_info(n)
            if artist_id:
                return artist_id, info
        return None, {}

    def get_artist_info(self, name):
        url = f"{self.url}/search"
        params = {'query': name,
            'type': 'artist',
            'limit': '5'}
        try:
            resp = self.get_page(url, params=params)
            if not resp:
                return None, {}
            soup = BeautifulSoup(resp.text, 'html.parser')
            links = soup.find_all('a', href=re.compile(r'/artist/[a-f0-9-]{36}'))
            for link in links:
                link_text = link.get_text(strip=True)
                if self.compare_names(name, link_text):
                    artist_id = link['href'].split('/')[-1]
                    info = self.get_artist_details(artist_id)
                    return artist_id, info
        except Exception as e:
            pass
        return None, {}

    def get_artist_details(self, artist_id):
        try:
            url = f"{self.url}/artist/{artist_id}"
            resp = self.get_page(url)
            if not resp:
                return {}
            soup = BeautifulSoup(resp.text, 'html.parser')
            info = {}
            type_elem = soup.find('dt', string='Type:')
            if type_elem:
                info['type'] = type_elem.find_next('dd').get_text(strip=True)
            gender_elem = soup.find('dt', string='Gender:')
            if gender_elem:
                info['gender'] = gender_elem.find_next('dd').get_text(strip=True)
            country_elem = soup.find('dt', string='Country:')
            if country_elem:
                info['country'] = country_elem.find_next('dd').get_text(strip=True)
            years_elem = soup.find('span', class_='small')
            if years_elem:
                years_text = years_elem.get_text(strip=True)
                year_match = re.search(r'(\d{4})', years_text)
                if year_match:
                    info['year'] = int(year_match.group(1))
            genre_elems = soup.find_all('a', href=re.compile(r'/genre/'))
            if genre_elems:
                info['genres'] = [g.get_text(strip=True) for g in genre_elems[:3]]
            return info
        except Exception as e:
            return {}

    def compare_names(self, name1, name2):
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()
        n1_clean = re.sub(r'^the\s+', '', n1)
        n2_clean = re.sub(r'^the\s+', '', n2)
        exact = n1 == n2
        clean_exact = n1_clean == n2_clean
        contains = n1 in n2 or n2 in n1
        clean_contains = n1_clean in n2_clean or n2_clean in n1_clean
        first_word = n1.split()[0] == n2.split()[0] if n1.split() and n2.split() else False
        return exact or clean_exact or contains or clean_contains or first_word

    def get_tracks(self, artist, artist_id, artist_info):
        tracks = []
        try:
            recordings = self.get_artist_recordings(artist, artist_id, artist_info)
            tracks.extend(recordings)
            works = self.get_works_tracks(artist, artist_id, artist_info)
            tracks.extend(works)
            releases = self.get_releases_tracks(artist, artist_id, artist_info)
            tracks.extend(releases)
            all_releases = self.get_all_releases_tracks(artist, artist_id, artist_info)
            tracks.extend(all_releases)
        except Exception as e:
            pass
        return tracks

    def get_artist_recordings(self, artist, artist_id, artist_info):
        tracks = []
        url = f"{self.url}/artist/{artist_id}/recordings"
        try:
            resp = self.get_page(url)
            if not resp:
                return tracks
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='tbl')
            if not table:
                return tracks
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    title_elem = cols[0].find('a') or cols[0]
                    title = title_elem.get_text(strip=True)
                    if title and len(title) > 1:
                        duration = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                        track_url = ""
                        url_link = cols[0].find('a', href=re.compile(r'/recording/[a-f0-9-]{36}'))
                        if url_link:
                            track_url = url_link['href']
                        features = self.get_track_features(title)
                        track = self.create_track_data(artist, artist_id, title, duration, track_url, features, artist_info, 'recordings')
                        if self.is_new_track(track):
                            tracks.append(track)
        except Exception as e:
            pass
        return tracks

    def get_works_tracks(self, artist, artist_id, artist_info, max_pages=10):
        tracks = []
        page = 0
        while page < max_pages:
            offset = page * 25
            url = f"{self.url}/artist/{artist_id}/works"
            params = {'offset': offset} if offset > 0 else {}
            try:
                resp = self.get_page(url, params=params)
                if not resp:
                    break
                soup = BeautifulSoup(resp.text, 'html.parser')
                table = soup.find('table', class_='tbl')
                if not table:
                    break
                page_tracks = 0
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 1:
                        title_elem = cols[0].find('a') or cols[0]
                        title = title_elem.get_text(strip=True)
                        if title and len(title) > 1:
                            features = self.get_track_features(title)
                            track = self.create_track_data(artist, artist_id, title, '', '', features, artist_info, 'works')
                            if self.is_new_track(track):
                                tracks.append(track)
                                page_tracks += 1
                next_link = soup.find('a', string='Next →')
                if not next_link:
                    break
                if page_tracks == 0:
                    break
                page += 1
                time.sleep(1)
            except Exception as e:
                break
        return tracks

    def get_releases_tracks(self, artist, artist_id, artist_info, max_releases=30):
        tracks = []
        try:
            releases = self.get_artist_releases(artist_id, max_releases)
            for i, (release_url, release_type) in enumerate(releases):
                release_tracks = self.parse_release(release_url, artist, artist_id, artist_info, release_type)
                tracks.extend(release_tracks)
                time.sleep(0.3)
        except Exception as e:
            pass
        return tracks

    def get_all_releases_tracks(self, artist, artist_id, artist_info, max_releases=50):
        tracks = []
        try:
            all_releases = self.get_all_artist_releases(artist_id, max_releases)
            for i, (release_url, release_type) in enumerate(all_releases[:30]):
                release_tracks = self.parse_release(release_url, artist, artist_id, artist_info, release_type)
                tracks.extend(release_tracks)
                time.sleep(0.3)
        except Exception as e:
            pass
        return tracks

    def get_artist_releases(self, artist_id, max_releases=30):
        releases = []
        page = 0
        while len(releases) < max_releases:
            offset = page * 25
            url = f"{self.url}/artist/{artist_id}/releases"
            params = {'offset': offset,
                'type': 'album|ep|single',
                'limit': 25}
            try:
                resp = self.get_page(url, params=params)
                if not resp:
                    break
                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = soup.find_all('tr')
                for row in rows[1:]:
                    if len(releases) >= max_releases:
                        break
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        link_elem = cols[0].find('a', href=re.compile(r'/release/[a-f0-9-]{36}'))
                        if link_elem:
                            release_url = f"{self.url}{link_elem['href']}"
                            release_type = cols[1].get_text(strip=True) if len(cols) > 1 else 'Unknown'
                            releases.append((release_url, release_type))
                next_link = soup.find('a', string='Next →')
                if not next_link:
                    break
                page += 1
            except Exception as e:
                break
        return releases

    def get_all_artist_releases(self, artist_id, max_releases=50):
        releases = []
        page = 0
        while len(releases) < max_releases:
            offset = page * 100
            url = f"{self.url}/artist/{artist_id}/releases"
            params = {'offset': offset,
                'type': 'album|ep|single|compilation|live|remix|soundtrack',
                'limit': 100}
            try:
                resp = self.get_page(url, params=params)
                if not resp:
                    break
                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = soup.find_all('tr')
                for row in rows[1:]:
                    if len(releases) >= max_releases:
                        break
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        link_elem = cols[0].find('a', href=re.compile(r'/release/[a-f0-9-]{36}'))
                        if link_elem:
                            release_url = f"{self.url}{link_elem['href']}"
                            release_type = cols[1].get_text(strip=True) if len(cols) > 1 else 'Unknown'
                            releases.append((release_url, release_type))
                next_link = soup.find('a', string='Next →')
                if not next_link:
                    break
                page += 1
            except Exception as e:
                break
        return releases

    def parse_release(self, release_url, artist, artist_id, artist_info, release_type):
        tracks = []
        try:
            resp = self.get_page(release_url)
            if not resp:
                return tracks
            soup = BeautifulSoup(resp.text, 'html.parser')
            title_elem = soup.find('h1')
            release_title = title_elem.get_text(strip=True) if title_elem else 'Unknown'
            tracklist = soup.find('table', class_='tbl')
            if not tracklist:
                return tracks
            track_number = 0
            for row in tracklist.find_all('tr'):
                if row.find('th'):
                    continue
                cols = row.find_all('td')
                if len(cols) >= 2:
                    track_number += 1
                    title_elem = cols[0].find('a') or cols[0]
                    title = title_elem.get_text(strip=True)
                    if title and len(title) > 1:
                        duration = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                        features = self.get_track_features(title)
                        track = self.create_track_data(artist, artist_id, title, duration, '', features, artist_info, 'releases')
                        track['release_title'] = release_title
                        track['release_type'] = release_type
                        track['track_number'] = track_number
                        if self.is_new_track(track):
                            tracks.append(track)
        except Exception as e:
            pass
        return tracks

    def create_track_data(self, artist, artist_id, title, duration, track_url, features, artist_info, source):
        return {'title': title,
            'artist': artist,
            'artist_id': artist_id,
            'duration': duration,
            'url': track_url,
            'feat': features['feat'],
            'remix': features['remix'],
            'live': features['live'],
            'original': features['original'],
            'acoustic': features['acoustic'],
            'instrumental': features['instrumental'],
            'cover': features['cover'],
            'explicit': features['explicit'],
            'extended': features['extended'],
            'demo': features['demo'],
            'artist_type': artist_info.get('type', 'Unknown'),
            'artist_gender': artist_info.get('gender', 'Unknown'),
            'artist_country': artist_info.get('country', 'Unknown'),
            'artist_year': artist_info.get('year', ''),
            'artist_genres': ', '.join(artist_info.get('genres', [])),
            'title_len': len(title),
            'title_words': len(title.split()),
            'source': source}

    def get_track_features(self, title):
        title_low = title.lower()
        return {'feat': 1 if any(x in title_low for x in ['feat.', 'ft.', 'with', '&', 'featuring']) else 0,
            'remix': 1 if 'remix' in title_low else 0,
            'live': 1 if 'live' in title_low else 0,
            'original': self.check_original(title_low),
            'acoustic': 1 if any(x in title_low for x in ['acoustic', 'unplugged']) else 0,
            'instrumental': 1 if any(x in title_low for x in ['instrumental', 'karaoke', 'beat']) else 0,
            'cover': 1 if any(x in title_low for x in ['cover', 'tribute to']) else 0,
            'explicit': 1 if any(x in title_low for x in ['explicit', 'uncensored', 'clean version', 'radio edit']) else 0,
            'extended': 1 if any(x in title_low for x in ['extended', 'extended version', 'long version']) else 0,
            'demo': 1 if any(x in title_low for x in ['demo', 'demo version', 'rough mix']) else 0}

    def check_original(self, title_low):
        not_original = ['remix', 'acoustic', 'cover', 'instrumental', 'live',
                        'demo', 'extended', 'edit', 'version', 'rework']
        if 'original' in title_low:
            return 1
        has_not_original = any(x in title_low for x in not_original)
        return 0 if has_not_original else 1

    def is_new_track(self, track):
        track_id = f"{track['artist'].lower()}_{track['title'].lower()}"
        track_id = re.sub(r'\s+', ' ', track_id).strip()
        if track_id in self.seen:
            return False
        self.seen.add(track_id)
        return True

    def remove_duplicates(self, tracks):
        unique = []
        seen_ids = set()
        for track in tracks:
            track_id = f"{track['artist'].lower()}_{track['title'].lower()}"
            track_id = re.sub(r'\s+', ' ', track_id).strip()
            if track_id not in seen_ids:
                seen_ids.add(track_id)
                unique.append(track)
        return unique


def main():
    artists = ['Bruno Mars', 'The Weeknd', 'Calvin Harris', 'Lady Gaga',
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
    parser = MusicParser()
    all_tracks = []
    for artist in artists:
        artist_id, info = parser.find_artist(artist)
        if artist_id:
            tracks = parser.get_tracks(artist, artist_id, info)
            all_tracks.extend(tracks)
        time.sleep(2)

    if all_tracks:
        unique_tracks = parser.remove_duplicates(all_tracks)
        df = pd.DataFrame(unique_tracks)
        try:
            df.to_csv('энц.csv', index=False, encoding='utf-8')
        except Exception as e:
            pass


if __name__ == "__main__":
    main()
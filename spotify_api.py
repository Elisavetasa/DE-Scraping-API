import time
import random
import csv
from typing import Optional, List, Dict, Any
from threading import Lock
from requests import get, post
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed


class RateLimitError(Exception):
    pass


class TokenError(Exception):
    pass


class APIError(Exception):
    pass


class Urls:
    def __init__(self, spotify: Optional[str] = None, other: Optional[Dict[str, Any]] = None):
        self.spotify = spotify
        self.other = other or {}

    def from_dict(d: Dict[str, Any]):
        if d is None:
            return Urls()
        spotify = d.get("spotify")
        other = {k: v for k, v in d.items() if k != "spotify"}
        return Urls(spotify=spotify, other=other)


class Image:
    def __init__(self, height: Optional[int], url: str, width: Optional[int]):
        self.height = height
        self.url = url
        self.width = width

    def from_dict(d: Dict[str, Any]):
        return Image(height=d.get("height"), url=d["url"], width=d.get("width"))


class Followers:
    def __init__(self, href: Optional[str], total: int):
        self.href = href
        self.total = total

    def from_dict(d: Dict[str, Any]):
        if d is None:
            return Followers(href=None, total=0)
        return Followers(href=d.get("href"), total=d.get("total", 0))


class Artist:
    def __init__(self, urls: Urls, href: str, id: str, name: str, type: str, uri: str,
                 popularity: int = 0, followers: Optional[Followers] = None, genres: Optional[list] = None):
        self.urls = urls
        self.href = href
        self.id = id
        self.name = name
        self.type = type
        self.uri = uri
        self.popularity = popularity
        self.followers = followers
        self.genres = genres or []

    def from_dict(d: dict):
        return Artist(
            urls=Urls.from_dict(d.get("external_urls") or {}),
            href=d.get("href", ""), id=d.get("id", ""), name=d.get("name", ""),
            type=d.get("type", ""), uri=d.get("uri", ""), popularity=d.get("popularity", 0),
            followers=Followers.from_dict(d.get("followers")) if d.get("followers") else None,
            genres=list(d.get("genres") or []))


class Album:
    def __init__(self, type: str, tracks_count: int, markets: List[str], urls: Urls, href: str,
                 id: str, images: List[Image], name: str, date: str, date_precision: str,
                 album_type: str, uri: str, artists: List[Artist]):
        self.type = type
        self.tracks_count = tracks_count
        self.markets = markets
        self.urls = urls
        self.href = href
        self.id = id
        self.images = images
        self.name = name
        self.date = date
        self.date_precision = date_precision
        self.album_type = album_type
        self.uri = uri
        self.artists = artists

    def from_dict(d: Dict[str, Any]):
        return Album(
            type=d.get("album_type", ""), tracks_count=d.get("total_tracks", 0),
            markets=list(d.get("available_markets") or []), urls=Urls.from_dict(d.get("external_urls") or {}),
            href=d.get("href", ""), id=d.get("id", ""), name=d.get("name", ""),
            images=[Image.from_dict(i) for i in d.get("images") or []], date=d.get("release_date", ""),
            date_precision=d.get("release_date_precision", ""), album_type=d.get("type", ""),
            uri=d.get("uri", ""), artists=[Artist.from_dict(a) for a in d.get("artists") or []])


class Track:
    def __init__(self, artists: List[Artist], markets: List[str], disc_num: int, duration: int,
                 explicit: bool, urls: Urls, href: str, id: str, name: str, preview: Optional[str],
                 track_num: int, type: str, uri: str, local: bool, popularity: int = 0):
        self.artists = artists
        self.markets = markets
        self.disc_num = disc_num
        self.duration = duration
        self.explicit = explicit
        self.urls = urls
        self.href = href
        self.id = id
        self.name = name
        self.preview = preview
        self.track_num = track_num
        self.type = type
        self.uri = uri
        self.local = local
        self.popularity = popularity

    def from_dict(d: Dict[str, Any]):
        return Track(
            artists=[Artist.from_dict(a) for a in d.get("artists") or []],
            markets=list(d.get("available_markets") or []), disc_num=d.get("disc_number", 0),
            duration=d.get("duration_ms", 0), explicit=d.get("explicit", False),
            urls=Urls.from_dict(d.get("external_urls") or {}), href=d.get("href", ""),
            id=d.get("id", ""), name=d.get("name", ""), preview=d.get("preview_url"),
            track_num=d.get("track_number", 0), type=d.get("type", ""), uri=d.get("uri", ""),
            local=d.get("is_local", False), popularity=d.get("popularity", 0))


token_lock = Lock()


class TokenManager:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self.token = None
        self.expiry = 0
        self.client_id = None
        self.secret = None
        self.load_token()
        self._initialized = True

    def load_token(self):
        try:
            with open("tokens.csv", "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                tokens = [(row[0], row[1]) for row in reader if len(row) >= 2]
            if not tokens:
                raise ValueError("Файл tokens.csv пуст!")

            self.client_id, self.secret = tokens[0]
            logger.info(f"Загружен 1 токен из tokens.csv")

        except FileNotFoundError:
            logger.error("Файл tokens.csv не найден!")
            raise

    def get_token(self) -> str:
        with token_lock:
            current_time = time.time()
            if self.token is None or current_time >= (self.expiry - 60):
                logger.debug("Обновление access token")
                self.token = self._fetch_token(self.client_id, self.secret)
                self.expiry = current_time + 3600
                logger.debug("Токен обновлён")
            return self.token

    def rotate(self) -> str:
        logger.warning("Обновление единственного токена")
        self.token = self._fetch_token(self.client_id, self.secret)
        self.expiry = time.time() + 3600
        return self.token

    def _fetch_token(self, client_id: str, secret: str) -> str:
        try:
            url = "https://accounts.spotify.com/api/token"
            logger.debug("Получение access token...")
            response = post(url,
                            data={"grant_type": "client_credentials"},
                            auth=(client_id, secret),
                            timeout=10)
            if response.status_code != 200:
                logger.error(f"Ошибка получения токена: {response.status_code} {response.text}")
                raise Exception(f"Ошибка получения токена: {response.status_code}")
            token = response.json()["access_token"]
            logger.debug(f"Access token получен: {token[:25]}...")
            return token
        except Exception as e:
            logger.error(f"Ошибка получения токена: {e}")
            raise


def log_req(method: str, url: str, code: int = None, text: str = None):
    if not hasattr(log_req, "counter"):
        log_req.counter = {"count": 0, "lock": Lock()}
    with log_req.counter["lock"]:
        log_req.counter["count"] += 1
        num = log_req.counter["count"]
    logger.debug(f"Запрос #{num}: {method} {url}")
    if code:
        if code == 200:
            logger.debug(f"Ответ: {code} OK")
        elif code == 429:
            logger.warning(f"Ответ: {code} RATE LIMIT!")
            if text:
                logger.warning(f"Тело ответа: {text[:200]}")
        elif code == 401:
            logger.warning(f"Ответ: {code} UNAUTHORIZED (токен истёк)")
            if text:
                logger.warning(f"Тело ответа: {text[:200]}")
        else:
            logger.error(f"Ответ: {code}")
            if text:
                logger.error(f"Тело ответа: {text[:200]}")


class SpotifyClient:
    def __init__(self, token_manager: TokenManager = None):
        self.token_manager = token_manager or TokenManager()
        self.delay = (0.5, 1.5)

    def _request(self, url: str) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token_manager.get_token()}"}
        sleep_time = random.uniform(*self.delay)
        logger.debug(f"Задержка перед запросом: {sleep_time:.2f}с")
        time.sleep(sleep_time)
        log_req("GET", url)
        response = get(url, headers=headers, timeout=10)
        log_req("GET", url, response.status_code, response.text if response.status_code != 200 else None)

        if response.status_code == 401:
            logger.warning("Токен истёк (401), обновляем...")
            self.token_manager.rotate()
            raise TokenError("Token expired (401)")
        if response.status_code == 429:
            logger.warning("Rate limit достигнут (429), делаем паузу...")
            time.sleep(60)
            raise RateLimitError("Rate limit reached")
        if response.status_code != 200:
            msg = f"Ошибка API: {response.status_code} - {response.text}"
            raise APIError(msg)
        return response.json()

    def find_artists(self, name: str) -> List[Artist]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"https://api.spotify.com/v1/search?q={name}&type=artist"
                data = self._request(url)
                if "artists" not in data or "items" not in data["artists"]:
                    logger.error(f"Некорректный ответ API при поиске артиста: {name}")
                    return []
                return [Artist.from_dict(artist) for artist in data["artists"]["items"]]
            except (TokenError, RateLimitError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для find_artists('{name}')")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"Все попытки исчерпаны для find_artists('{name}'): {e}")
                    raise

    def get_artist(self, artist_id: str) -> Optional[Artist]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"https://api.spotify.com/v1/artists/{artist_id}"
                data = self._request(url)
                return Artist.from_dict(data)
            except (TokenError, RateLimitError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для get_artist('{artist_id}')")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"Все попытки исчерпаны для get_artist('{artist_id}'): {e}")
                    raise

    def get_albums(self, artist_id: str, max_albums: Optional[int] = None) -> List[Album]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                all_albums = []
                url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=50"
                while url:
                    data = self._request(url)
                    albums = [Album.from_dict(album) for album in data.get("items", [])]
                    all_albums.extend(albums)
                    if max_albums and len(all_albums) >= max_albums:
                        all_albums = all_albums[:max_albums]
                        logger.info(f"Достигнут лимит альбомов: {max_albums}")
                        break
                    url = data.get("next")
                return all_albums
            except (TokenError, RateLimitError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для get_albums('{artist_id}')")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"Все попытки исчерпаны для get_albums('{artist_id}'): {e}")
                    raise

    def get_tracks(self, album_id: str) -> List[Track]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                all_tracks = []
                url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
                while url:
                    data = self._request(url)
                    all_tracks.extend([Track.from_dict(track) for track in data.get("items", [])])
                    url = data.get("next")
                return all_tracks
            except (TokenError, RateLimitError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для get_tracks('{album_id}')")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"Все попытки исчерпаны для get_tracks('{album_id}'): {e}")
                    raise

    def get_track(self, track_id: str) -> Optional[Track]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"https://api.spotify.com/v1/tracks/{track_id}"
                data = self._request(url)
                return Track.from_dict(data)
            except (TokenError, RateLimitError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} для get_track('{track_id}')")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"Все попытки исчерпаны для get_track('{track_id}'): {e}")
                    raise


def process_track(client: SpotifyClient, track: Track) -> Track:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            track_details = client.get_track(track.id)
            if track_details:
                return track_details
            return track
        except (RateLimitError, TokenError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Попытка {attempt + 1}/{max_retries} для трека {track.name}")
                time.sleep(10)
                continue
            else:
                logger.error(f"Не удалось получить детали трека {track.name} (ID: {track.id}): {e}")
                return track
        except APIError as e:
            logger.error(f"Ошибка API при обработке трека {track.name} (ID: {track.id}): {e}")
            return track
        except Exception as e:
            logger.error(f"Неизвестная ошибка при обработке трека {track.name}: {e}")
            return track


def process_album(lock: Lock, client: SpotifyClient, album: Album, artist: Artist, file: str, max_workers: int = 5):
    logger.info(f"Обработка альбома: {album.name}")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            tracks = client.get_tracks(album.id)
            break
        except (RateLimitError, TokenError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Попытка {attempt + 1}/{max_retries} для альбома {album.name}")
                time.sleep(10)
                continue
            else:
                logger.error(f"Не удалось получить треки для альбома {album.name}: {e}")
                return {album.id: []}
        except APIError as e:
            logger.error(f"Не удалось получить треки для альбома {album.name}: {e}")
            return {album.id: []}
    if not tracks:
        logger.warning(f"Альбом '{album.name}' не содержит треков.")
        return {album.id: []}
    detailed_tracks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_track = {
            executor.submit(process_track, client, track): track
            for track in tracks}
        for future in as_completed(future_to_track):
            try:
                track_details = future.result()
                detailed_tracks.append(track_details)
                logger.debug(f"Обработан трек: {track_details.name}")
            except Exception as e:
                original_track = future_to_track[future]
                logger.error(f"Неизвестная ошибка в потоке трека {original_track.name}: {e}")
    detailed_tracks.sort(key=lambda t: t.track_num)
    save_csv(lock, artist, [album], {album.id: detailed_tracks}, file)
    return {album.id: detailed_tracks}


def process_artist(lock: Lock, client: SpotifyClient, name: str, file: str, albums_workers: int = 2,
                   tracks_workers: int = 5, max_albums: Optional[int] = None):
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Обработка артиста: {name}")
    logger.info(f"{'=' * 60}")
    try:
        artists = client.find_artists(name)
        if not artists:
            logger.warning(f"Исполнитель '{name}' не найден")
            return
        artist = artists[0]
        logger.success(f"Найден исполнитель '{name}' (ID: {artist.id})")
        artist_data = client.get_artist(artist.id)
        if not artist_data:
            logger.error(f"Не удалось получить детали артиста '{name}' (API вернул null)")
            return
        logger.info(f"Подписчиков: {artist_data.followers.total:,}")
        logger.info(f"Популярность: {artist_data.popularity}/100")
        if artist_data.genres:
            logger.info(f"Жанры: {', '.join(artist_data.genres)}")
        albums = client.get_albums(artist.id, max_albums)
        logger.success(f"Найдено {len(albums)} альбомов (до фильтрации)")
        original_count = len(albums)
        filtered_albums = [a for a in albums if a.type == "album"]
        compilations_skipped = original_count - len(filtered_albums)
        final_albums = [a for a in filtered_albums if a.tracks_count > 1]
        singles_skipped = len(filtered_albums) - len(final_albums)
        albums = final_albums
        if compilations_skipped > 0:
            logger.info(f"Отфильтровано {compilations_skipped} релизов (тип не 'album')")
        if singles_skipped > 0:
            logger.info(f"Отфильтровано {singles_skipped} синглов (1 трек)")
        logger.success(f"Осталось {len(albums)} альбомов для обработки")
        if not albums:
            logger.warning(f"Для артиста '{name}' не осталось альбомов после фильтрации.")
            return
        with ThreadPoolExecutor(max_workers=albums_workers) as executor:
            futures = [
                executor.submit(
                    process_album,
                    lock,
                    client,
                    album,
                    artist_data,
                    file,
                    tracks_workers)
                for album in albums]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Неизвестная ошибка в потоке альбома: {e}")
        logger.success(f"Обработка артиста '{name}' завершена")
    except RateLimitError:
        logger.error(f"Обработка артиста '{name}' прервана: достигнут Rate Limit (все токены исчерпаны).")
    except APIError as e:
        logger.error(f"Обработка артиста '{name}' прервана (Ошибка API): {e}")
    except Exception as e:
        logger.exception(f"Критическая неизвестная ошибка при обработке '{name}': {e}")


def process_artist_retry(lock: Lock, client: SpotifyClient, name: str, file: str, albums_workers: int,
                         tracks_workers: int, max_albums: int, max_retries: int = 5) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                logger.warning(f"Повтор {attempt}/{max_retries} для '{name}'")
                time.sleep(5)
            process_artist(
                lock,
                client,
                name,
                file,
                albums_workers,
                tracks_workers,
                max_albums)
            if attempt > 1:
                logger.success(f"Успешно обработан при повторе {attempt}: {name}")
            return True
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"Попытка {attempt}/{max_retries} не удалась для '{name}': {e}")
            else:
                logger.error(f"ВСЕ {max_retries} попыток исчерпаны для '{name}': {e}")
                return False
    return False


def save_csv(lock: Lock, artist: Artist, albums: List[Album], all_tracks: Dict[str, List[Track]],
             file: str = "spotify_data.csv"):
    fieldnames = ["Сервис", "Исполнитель", "Количество подписчиков", "Популярность исполнителя", "Жанры",
                  "Название альбома", "Тип альбома", "Год выпуска", "Количество треков", "Название трека",
                  "Номер трека", "Продолжительность (мс)", "Продолжительность (мин:сек)", "Популярность трека",
                  "Наличие матов", "Spotify URL (альбом)", "Spotify URL (трек)"]
    with lock:
        file_exists = False
        try:
            with open(file, "r", encoding="utf-8") as csvfile:
                file_exists = True
        except FileNotFoundError:
            pass
        with open(file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for album in albums:
                tracks = all_tracks.get(album.id, [])
                for track in tracks:
                    duration_min = track.duration // 60000
                    duration_sec = (track.duration % 60000) // 1000
                    writer.writerow({
                        "Сервис": "Spotify",
                        "Исполнитель": artist.name,
                        "Количество подписчиков": artist.followers.total if artist.followers else 0,
                        "Популярность исполнителя": artist.popularity,
                        "Жанры": ", ".join(artist.genres) if artist.genres else "",
                        "Название альбома": album.name,
                        "Тип альбома": album.type,
                        "Год выпуска": album.date[:4] if album.date else "",
                        "Количество треков": album.tracks_count,
                        "Название трека": track.name,
                        "Номер трека": track.track_num,
                        "Продолжительность (мс)": track.duration,
                        "Продолжительность (мин:сек)": f"{duration_min}:{duration_sec:02d}",
                        "Популярность трека": track.popularity,
                        "Наличие матов": "Да" if track.explicit else "Нет",
                        "Spotify URL (альбом)": album.urls.spotify or "",
                        "Spotify URL (трек)": track.urls.spotify or ""})
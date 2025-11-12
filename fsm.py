from requests import get, post
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
import random
from loguru import logger

csv_lock = Lock()
token_lock = Lock()
req_counter = {"count": 0, "lock": Lock()}

logger.remove()
logger.add("spotify.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
           level="DEBUG",
           rotation="10 MB",
           compression="zip",
           encoding="utf-8")
logger.add(lambda msg: print(msg, end=""),
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
           level="DEBUG",
           colorize=True)


def log_req(method: str, url: str, code: int = None, text: str = None):
    with req_counter["lock"]:
        req_counter["count"] += 1
        num = req_counter["count"]
    logger.debug(f"Запрос #{num}: {method} {url}")
    if code and code != 200:  # Только ошибки
        if code == 429:
            logger.warning(f"Ответ: {code} LIMIT!")
        elif code == 401:
            logger.warning(f"Ответ: {code} TOKEN ERROR")
        else:
            logger.error(f"Ответ: {code}")
        if text:
            logger.warning(f"Текст: {text[:200]}")


class TokenManager:
    def __init__(self):
        self.tokens = []
        self.token = None
        self.expiry = 0
        self.load_tokens()

    def load_tokens(self):
        try:
            with open("tokens.csv", "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                self.tokens = [(row[0], row[1]) for row in reader if len(row) >= 2]
            if not self.tokens:
                raise ValueError("Файл tokens.csv пуст!")
            logger.info(f"Загружено {len(self.tokens)} токенов")
        except FileNotFoundError:
            logger.error("Файл tokens.csv не найден!")
            raise

    def get_token(self) -> str:
        with token_lock:
            now = time.time()
            if self.token is None or now >= (self.expiry - 60):
                logger.debug("Обновление токена")
                client_id, secret = self.tokens[0]  # Всегда первый токен
                self.token = self._get_token(client_id, secret)
                self.expiry = now + 3600
                logger.debug("Токен обновлен")
            return self.token

    def _get_token(self, client_id: str, secret: str) -> str:
        try:
            url = "https://accounts.spotify.com/api/token"
            logger.debug("Получение токена...")
            log_req("POST", url)
            response = post(url,
                            data={"grant_type": "client_credentials"},
                            auth=(client_id, secret),
                            timeout=10)
            log_req("POST",
                    url,
                    response.status_code,
                    response.text if response.status_code != 200 else None)
            if response.status_code != 200:
                logger.error(f"Ошибка токена: {response.status_code} {response.text}")
                raise Exception(f"Ошибка токена: {response.status_code}")
            token = response.json()["access_token"]
            logger.debug(f"Токен получен: {token[:30]}...")
            return token
        except Exception as e:
            logger.error(f"Ошибка получения токена: {e}")
            raise


token_manager = None


@dataclass
class Urls:
    spotify: Optional[str] = None
    other: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Urls":
        if d is None:
            return cls()
        spotify = d.get("spotify")
        other = {k: v for k, v in d.items() if k != "spotify"}
        return cls(spotify=spotify, other=other)


@dataclass
class Image:
    height: Optional[int]
    url: str
    width: Optional[int]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Image":
        return cls(height=d.get("height"),
                   url=d["url"],
                   width=d.get("width"))


@dataclass
class Followers:
    href: Optional[str]
    total: int

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Followers":
        if d is None:
            return cls(href=None, total=0)
        return cls(href=d.get("href"), total=d.get("total", 0))


@dataclass
class Artist:
    urls: Urls
    href: str
    id: str
    name: str
    type: str
    uri: str
    popularity: int = 0
    followers: Optional[Followers] = None
    genres: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> "Artist":
        return cls(urls=Urls.from_dict(d.get("external_urls") or {}),
                   href=d.get("href", ""),
                   id=d.get("id", ""),
                   name=d.get("name", ""),
                   type=d.get("type", ""),
                   uri=d.get("uri", ""),
                   popularity=d.get("popularity", 0),
                   followers=Followers.from_dict(d.get("followers"))
                   if d.get("followers")
                   else None,
                   genres=list(d.get("genres") or []))


@dataclass
class Album:
    type: str
    tracks_count: int
    markets: List[str]
    urls: Urls
    href: str
    id: str
    images: List[Image]
    name: str
    date: str
    date_precision: str
    uri: str
    artists: List[Artist]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Album":
        return cls(type=d.get("album_type", ""),
                   tracks_count=d.get("total_tracks", 0),
                   markets=list(d.get("available_markets") or []),
                   urls=Urls.from_dict(d.get("external_urls") or {}),
                   href=d.get("href", ""),
                   id=d.get("id", ""),
                   images=[Image.from_dict(i) for i in d.get("images") or []],
                   name=d.get("name", ""),
                   date=d.get("release_date", ""),
                   date_precision=d.get("release_date_precision", ""),
                   uri=d.get("uri", ""),
                   artists=[Artist.from_dict(a) for a in d.get("artists") or []])


@dataclass
class Track:
    artists: List[Artist]
    markets: List[str]
    disc_num: int
    duration: int
    explicit: bool
    urls: Urls
    href: str
    id: str
    name: str
    preview: Optional[str]
    track_num: int
    type: str
    uri: str
    local: bool
    popularity: int = 0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Track":
        return cls(artists=[Artist.from_dict(a) for a in d.get("artists") or []],
                   markets=list(d.get("available_markets") or []),
                   disc_num=d.get("disc_number", 0),
                   duration=d.get("duration_ms", 0),
                   explicit=d.get("explicit", False),
                   urls=Urls.from_dict(d.get("external_urls") or {}),
                   href=d.get("href", ""),
                   id=d.get("id", ""),
                   name=d.get("name", ""),
                   preview=d.get("preview_url"),
                   track_num=d.get("track_number", 0),
                   type=d.get("type", ""),
                   uri=d.get("uri", ""),
                   local=d.get("is_local", False),
                   popularity=d.get("popularity", 0))


class LimitError(Exception):
    pass


class TokenError(Exception):
    pass


class APIError(Exception):
    pass


def find_artists(token: str, name: str) -> list[Artist]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/search?q={name}&type=artist"
    sleep_time = random.uniform(0.5, 1.5)
    logger.debug(f"Задержка: {sleep_time:.2f}с")
    time.sleep(sleep_time)
    log_req("GET", url)
    response = get(url, headers=headers, timeout=10)
    log_req("GET",
            url,
            response.status_code,
            response.text if response.status_code != 200 else None)
    if response.status_code == 401:
        raise TokenError("Token expired (401)")
    if response.status_code == 429:
        raise LimitError("Rate limit reached")
    if response.status_code != 200:
        msg = f"Ошибка поиска {name}: {response.status_code} - {response.text}"
        raise APIError(msg)
    data = response.json()
    if "artists" not in data or "items" not in data["artists"]:
        logger.error(f"Некорректный ответ для: {name}")
        return []
    return [Artist.from_dict(artist) for artist in data["artists"]["items"]]


def get_artist(token: str, artist_id: str) -> Optional[Artist]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    sleep_time = random.uniform(0.5, 1.5)
    logger.debug(f"Задержка: {sleep_time:.2f}с")
    time.sleep(sleep_time)
    log_req("GET", url)
    response = get(url, headers=headers, timeout=10)
    log_req("GET",
            url,
            response.status_code,
            response.text if response.status_code != 200 else None)
    if response.status_code == 401:
        raise TokenError("Token expired (401)")
    if response.status_code == 429:
        raise LimitError("Rate limit reached")
    if response.status_code != 200:
        msg = f"Ошибка артиста {artist_id}: {response.status_code} - {response.text}"
        raise APIError(msg)
    return Artist.from_dict(response.json())


def get_albums(token: str, artist_id: str, max_albums: Optional[int] = None) -> list[Album]:
    headers = {"Authorization": f"Bearer {token}"}
    albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=50"
    while url:
        sleep_time = random.uniform(0.5, 2)
        logger.debug(f"Задержка: {sleep_time:.2f}с")
        time.sleep(sleep_time)
        log_req("GET", url)
        response = get(url, headers=headers, timeout=10)
        log_req("GET",
                url,
                response.status_code,
                response.text if response.status_code != 200 else None)
        if response.status_code == 401:
            raise TokenError("Token expired (401)")
        if response.status_code == 429:
            raise LimitError("Rate limit reached")
        if response.status_code != 200:
            msg = f"Ошибка альбомов {artist_id}: {response.status_code} - {response.text}"
            logger.error(msg)
            raise APIError(msg)
        data = response.json()
        albums.extend([Album.from_dict(album) for album in data.get("items", [])])
        if max_albums and len(albums) >= max_albums:
            albums = albums[:max_albums]
            logger.info(f"Лимит альбомов: {max_albums}")
            break
        url = data.get("next")
    return albums


def get_tracks(token: str, album_id: str) -> list[Track]:
    headers = {"Authorization": f"Bearer {token}"}
    tracks = []
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
    while url:
        sleep_time = random.uniform(0.5, 2)
        logger.debug(f"Задержка: {sleep_time:.2f}с")
        time.sleep(sleep_time)
        log_req("GET", url)
        response = get(url, headers=headers, timeout=10)
        log_req("GET",
                url,
                response.status_code,
                response.text if response.status_code != 200 else None)
        if response.status_code == 401:
            raise TokenError("Token expired (401)")
        if response.status_code == 429:
            raise LimitError("Rate limit reached")
        if response.status_code != 200:
            msg = f"Ошибка треков {album_id}: {response.status_code} - {response.text}"
            logger.error(msg)
            raise APIError(msg)
        data = response.json()
        tracks.extend([Track.from_dict(track) for track in data.get("items", [])])
        url = data.get("next")
    return tracks


def get_track(token: str, track_id: str) -> Optional[Track]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    sleep_time = random.uniform(0.5, 1.5)
    logger.debug(f"Задержка: {sleep_time:.2f}с")
    time.sleep(sleep_time)
    log_req("GET", url)
    response = get(url, headers=headers, timeout=10)
    log_req("GET",
            url,
            response.status_code,
            response.text if response.status_code != 200 else None)
    if response.status_code == 401:
        raise TokenError("Token expired (401)")
    if response.status_code == 429:
        raise LimitError("Rate limit reached")
    if response.status_code != 200:
        msg = f"Ошибка трека {track_id}: {response.status_code} - {response.text}"
        raise APIError(msg)
    return Track.from_dict(response.json())


def process_track(token: str, track: Track) -> Track:
    try:
        details = get_track(token, track.id)
        if details:
            return details
        return track
    except (LimitError, TokenError):
        logger.error(f"Ошибка трека {track.name} (ID: {track.id}): Токены исчерпаны.")
        return track
    except APIError as e:
        logger.error(f"Ошибка API трека {track.name} (ID: {track.id}): {e}")
        return track
    except Exception as e:
        logger.error(f"Ошибка трека {track.name}: {e}")
        return track


def process_album(
        token: str,
        album: Album,
        artist: Artist,
        file: str,
        workers: int = 5,
) -> Dict[str, List[Track]]:
    logger.info(f"Альбом: {album.name}")
    try:
        tracks = get_tracks(token, album.id)
    except (LimitError, TokenError):
        logger.error(f"Не удалось получить треки альбома {album.name}: Токены исчерпаны.")
        return {album.id: []}
    except APIError as e:
        logger.error(f"Не удалось получить треки альбома {album.name}: {e}")
        return {album.id: []}
    if not tracks:
        logger.warning(f"Альбом '{album.name}' без треков.")
        return {album.id: []}
    detailed = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_track = {executor.submit(process_track, token, track): track for track in tracks}
        for future in as_completed(future_to_track):
            try:
                track_details = future.result()
                detailed.append(track_details)
                logger.debug(f"Обработан трек: {track_details.name}")
            except Exception as e:
                original_track = future_to_track[future]
                logger.error(f"Ошибка трека {original_track.name}: {e}")
    detailed.sort(key=lambda t: t.track_num)
    save_csv(artist, [album], {album.id: detailed}, file)
    return {album.id: detailed}


def save_csv(
        artist: Artist,
        albums: List[Album],
        tracks: Dict[str, List[Track]],
        file: str = "spotify_data.csv",
):
    fieldnames = ["Сервис",
                  "Исполнитель",
                  "Количество подписчиков",
                  "Популярность исполнителя",
                  "Жанры",
                  "Название альбома",
                  "Тип альбома",
                  "Год выпуска",
                  "Количество треков",
                  "Название трека",
                  "Номер трека",
                  "Продолжительность (мс)",
                  "Продолжительность (мин:сек)",
                  "Популярность трека",
                  "Наличие матов",
                  "Spotify URL (альбом)",
                  "Spotify URL (трек)"]
    with csv_lock:
        exists = False
        try:
            with open(file, "r", encoding="utf-8") as f:
                exists = True
        except FileNotFoundError:
            pass
        with open(file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                writer.writeheader()
            for album in albums:
                album_tracks = tracks.get(album.id, [])
                for track in album_tracks:
                    min = track.duration // 60000
                    sec = (track.duration % 60000) // 1000
                    writer.writerow({"Сервис": "Spotify",
                                     "Исполнитель": artist.name,
                                     "Количество подписчиков": artist.followers.total
                                     if artist.followers
                                     else 0,
                                     "Популярность исполнителя": artist.popularity,
                                     "Жанры": ", ".join(artist.genres)
                                     if artist.genres
                                     else "",
                                     "Название альбома": album.name,
                                     "Тип альбома": album.type,
                                     "Год выпуска": album.date[:4]
                                     if album.date
                                     else "",
                                     "Количество треков": album.tracks_count,
                                     "Название трека": track.name,
                                     "Номер трека": track.track_num,
                                     "Продолжительность (мс)": track.duration,
                                     "Продолжительность (мин:сек)": f"{min}:{sec:02d}",
                                     "Популярность трека": track.popularity,
                                     "Наличие матов": "Да" if track.explicit else "Нет",
                                     "Spotify URL (альбом)": album.urls.spotify
                                     if album.urls.spotify
                                     else "",
                                     "Spotify URL (трек)": track.urls.spotify
                                     if track.urls.spotify
                                     else ""})


def process_artist(
        token: str,
        name: str,
        file: str,
        album_workers: int = 2,
        track_workers: int = 5,
        max_albums: Optional[int] = None,
):
    logger.info(f"Артист: {name}")
    try:
        artists = find_artists(token, name)
        if not artists:
            logger.warning(f"Артист '{name}' не найден")
            return
        artist = artists[0]
        logger.success(f"Найден артист '{name}' (ID: {artist.id})")
        artist_data = get_artist(token, artist.id)
        if not artist_data:
            logger.error(f"Не удалось получить данные артиста '{name}'")
            return
        logger.info(f"Подписчиков: {artist_data.followers.total:,}")
        logger.info(f"Популярность: {artist_data.popularity}/100")
        if artist_data.genres:
            logger.info(f"Жанры: {', '.join(artist_data.genres)}")
        albums = get_albums(token, artist.id, max_albums)
        logger.success(f"Найдено {len(albums)} альбомов (до фильтрации)")

        # Упрощенная фильтрация
        filtered_albums = [a for a in albums if a.type == "album"]
        albums = [a for a in filtered_albums if a.tracks_count > 1]

        logger.success(f"Осталось {len(albums)} альбомов")
        if not albums:
            logger.warning(f"Для артиста '{name}' нет альбомов после фильтрации.")
            return
        with ThreadPoolExecutor(max_workers=album_workers) as executor:
            futures = [executor.submit(
                process_album,
                token,
                album,
                artist_data,
                file,
                track_workers)
                for album in albums]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка альбома: {e}")
        logger.success(f"Артист '{name}' завершен")
    except LimitError:
        logger.error(f"Артист '{name}' ПРЕРВАН: Rate Limit.")
    except APIError as e:
        logger.error(f"Артист '{name}' ПРЕРВАН (Ошибка API): {e}")
    except Exception as e:
        logger.exception(f"Ошибка артиста '{name}': {e}")


def main():
    global token_manager
    ARTISTS = 3
    ALBUMS = 5
    TRACKS = 10
    MAX_ALBUMS = None
    try:
        token_manager = TokenManager()
        if len(token_manager.tokens) < ARTISTS:
            logger.warning(f"Токенов ({len(token_manager.tokens)}) меньше чем воркеров ({ARTISTS})")
        token = token_manager.get_token()
        logger.success(f"Токен получен\n")
    except Exception as e:
        logger.exception(f"Ошибка токена: {e}")
        return
    try:
        with open("artists.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            try:
                next(reader)
            except StopIteration:
                logger.error("Файл artists.csv пуст!")
                return
            names = [row[1] for row in reader if len(row) > 1 and row[1].strip()]
    except FileNotFoundError:
        logger.error("Файл artists.csv не найден!")
        return
    except Exception as e:
        logger.error(f"Ошибка чтения artists.csv: {e}")
        return
    if not names:
        logger.error("Нет артистов для обработки.")
        return
    logger.info(f"Найдено {len(names)} артистов")
    file = "spotify_data.csv"
    start = time.time()

    if ARTISTS == 1:
        for name in names:
            process_artist(token, name, file, ALBUMS, TRACKS, MAX_ALBUMS)
    else:
        with ThreadPoolExecutor(max_workers=ARTISTS) as executor:
            futures = [executor.submit(
                process_artist,
                token,
                name,
                file,
                ALBUMS,
                TRACKS,
                MAX_ALBUMS)
                for name in names]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в пуле: {e}")
    logger.info(f"Файл: {file}")


if __name__ == "__main__":
    main()
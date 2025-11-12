import csv
import time
from loguru import logger
from requests import get
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from spotify_api1 import SpotifyClient

logger.remove()
logger.add(lambda msg: print(msg, end=""),
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
    colorize=True)
cache_lock = Lock()


def clean_title(title: str) -> str:
    title = title.strip().lstrip("*").strip()
    title = re.sub(r"\s*\(feat\..*?\)", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(ft\..*?\)", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(with.*?\)", "", title, flags=re.IGNORECASE)
    return title.lower()


def parse_streams(text: str) -> int:
    try:
        return int(text.replace(",", ""))
    except:
        return 0


def get_artist_id(client: SpotifyClient, name: str) -> str:
    try:
        artists = client.find_artists(name)
        if artists and len(artists) > 0:
            return artists[0].id
    except Exception as e:
        logger.warning(f"Ошибка поиска артиста {name}: {e}")
    return None


def get_artist_data(artist_id: str) -> tuple:
    try:
        url = f"https://kworb.net/spotify/artist/{artist_id}_songs.html"
        response = get(url, timeout=15)
        if response.status_code != 200:
            logger.warning(f"Ошибка {response.status_code} для {artist_id}")
            return {}, {}
        soup = BeautifulSoup(response.text, "html.parser")
        stats = {}
        stats_table = soup.select_one("body > div > div:nth-child(5) > table:nth-child(6)")
        if stats_table:
            rows = stats_table.find_all("tr")
            if len(rows) >= 2:
                streams_row = rows[1].find_all("td")
                if len(streams_row) >= 5:
                    stats["total_streams"] = parse_streams(streams_row[1].text.strip())
                    stats["as_lead_streams"] = parse_streams(streams_row[2].text.strip())
                    stats["solo_streams"] = parse_streams(streams_row[3].text.strip())
                    stats["as_feature_streams"] = parse_streams(streams_row[4].text.strip())
                if len(rows) >= 4:
                    tracks_row = rows[3].find_all("td")
                    if len(tracks_row) >= 5:
                        stats["total_tracks"] = parse_streams(tracks_row[1].text.strip())
                        stats["as_lead_tracks"] = parse_streams(tracks_row[2].text.strip())
                        stats["solo_tracks"] = parse_streams(tracks_row[3].text.strip())
                        stats["as_feature_tracks"] = parse_streams(tracks_row[4].text.strip())
            logger.success(f"Получена статистика артиста")
        else:
            logger.warning(f"Таблица статистики не найдена для {artist_id}")
        table = soup.find("table", class_="addpos")
        if not table:
            logger.warning(f"Таблица треков не найдена для {artist_id}")
            return stats, {}
        streams = {}
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 3:
                title_cell = cols[0].find("a")
                if title_cell:
                    track_name = clean_title(title_cell.text)
                    streams[track_name] = {"total": parse_streams(cols[1].text.strip()),
                        "daily": parse_streams(cols[2].text.strip())}
        return stats, streams
    except Exception as e:
        logger.error(f"Ошибка парсинга для {artist_id}: {e}")
        return {}, {}


def load_listeners(file: str) -> dict:
    cache = {}
    try:
        with open(file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row["name"].strip()
                cache[name] = parse_streams(row["listeners"])
        logger.success(f"Загружено {len(cache)} артистов из {file}")
    except FileNotFoundError:
        logger.warning(f"Файл {file} не найден. Прослушивания артистов не будут добавлены.")
    except Exception as e:
        logger.error(f"Ошибка чтения {file}: {e}")
    return cache


def process_artist(
        name: str,
        tracks: list,
        client: SpotifyClient,
        id_cache: dict,
        streams_cache: dict,
        stats_cache: dict,
):
    updated = 0
    not_found = 0
    good_tracks = []
    with cache_lock:
        artist_id = id_cache.get(name)
    if artist_id is None:
        artist_id = get_artist_id(client, name)
        with cache_lock:
            id_cache[name] = artist_id
        if not artist_id:
            logger.warning(f"Artist ID не найден, пропускаем")
            return 0, len(tracks), good_tracks
        logger.success(f"Найден ID: {artist_id}")
    with cache_lock:
        stats = stats_cache.get(artist_id)
        streams = streams_cache.get(artist_id)
    if streams is None or stats is None:
        stats, streams = get_artist_data(artist_id)
        with cache_lock:
            stats_cache[artist_id] = stats
            streams_cache[artist_id] = streams
        logger.success(f"Загружено {len(streams)} треков с kworb.net")
        time.sleep(0.5)
    matched = 0
    for track in tracks:
        track_name = track["Название трека"]
        clean_name = clean_title(track_name)
        track["Прослушивания (общие)"] = ""
        track["Прослушивания (ежедневные)"] = ""
        track["Total Streams"] = stats.get("total_streams", "")
        track["Total Tracks"] = stats.get("total_tracks", "")
        track["As Lead Streams"] = stats.get("as_lead_streams", "")
        track["As Lead Tracks"] = stats.get("as_lead_tracks", "")
        track["Solo Streams"] = stats.get("solo_streams", "")
        track["Solo Tracks"] = stats.get("solo_tracks", "")
        track["As Feature Streams"] = stats.get("as_feature_streams", "")
        track["As Feature Tracks"] = stats.get("as_feature_tracks", "")
        found = False
        if clean_name in streams:
            stream_info = streams[clean_name]
            found = True
        else:
            for kworb_name, kworb_data in streams.items():
                if clean_name in kworb_name or kworb_name in clean_name:
                    stream_info = kworb_data
                    found = True
                    break
        if found:
            track["Прослушивания (общие)"] = stream_info["total"]
            track["Прослушивания (ежедневные)"] = stream_info["daily"]
            matched += 1
            updated += 1
            good_tracks.append(track)
        else:
            not_found += 1
    logger.info(f"Совпадений: {matched}/{len(tracks)}")
    return updated, not_found, good_tracks


def main():
    INPUT = "spotify_data.csv"
    ARTISTS = "artists.csv"
    OUTPUT = "spotify_data_with_streams.csv"
    WORKERS = 10
    logger.info("Добавление данных о прослушиваниях")
    try:
        client = SpotifyClient()
        logger.success("Получен токен Spotify API")
    except Exception as e:
        logger.error(f"Ошибка получения токена: {e}")
        return
    try:
        with open(INPUT, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fields = list(reader.fieldnames)
    except FileNotFoundError:
        logger.error(f"Файл {INPUT} не найден!")
        return
    if not rows:
        logger.error("CSV файл пуст!")
        return
    listeners_cache = load_listeners(ARTISTS)
    new_fields = ["Прослушивания (общие)",
        "Прослушивания (ежедневные)",
        "Общие прослушивания артиста",
        "Total Streams",
        "Total Tracks",
        "As Lead Streams",
        "As Lead Tracks",
        "Solo Streams",
        "Solo Tracks",
        "As Feature Streams",
        "As Feature Tracks"]
    for field in new_fields:
        if field not in fields:
            if "Spotify URL (альбом)" in fields:
                pos = fields.index("Spotify URL (альбом)")
                fields.insert(pos + 1, field)
            else:
                fields.append(field)
    logger.info(f"Загружено {len(rows)} треков из CSV\n")
    artists_data = {}
    for row in rows:
        name = row["Исполнитель"]
        if name not in artists_data:
            artists_data[name] = []
        artists_data[name].append(row)
    logger.info(f"Найдено {len(artists_data)} уникальных артистов")
    id_cache = {}
    streams_cache = {}
    stats_cache = {}
    total_updated = 0
    total_not_found = 0
    result_rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(
                process_artist,
                name,
                tracks,
                client,
                id_cache,
                streams_cache,
                stats_cache,
            ): name
            for name, tracks in artists_data.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                updated, not_found, good_tracks = future.result()
                listeners = listeners_cache.get(name, "")
                for track in good_tracks:
                    track["Общие прослушивания артиста"] = listeners
                result_rows.extend(good_tracks)
                total_updated += updated
                total_not_found += not_found
            except Exception as e:
                logger.error(f"Ошибка обработки {name}: {e}")
    logger.info(f"Сохранение результата в {OUTPUT}...")
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result_rows)


if __name__ == "__main__":
    main()
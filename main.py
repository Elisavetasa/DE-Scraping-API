import csv
import time
from loguru import logger
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from spotify_api1 import SpotifyClient, process_artist_retry

csv_lock = Lock()


def setup_logs():
    logger.remove()
    logger.add("fetch_spotify_mt.log",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
               level="DEBUG",
               rotation="10 MB",
               compression="zip",
               encoding="utf-8")
    logger.add(lambda msg: print(msg, end=""),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="DEBUG",
        colorize=True)


def main():
    setup_logs()
    ARTISTS_WORKERS = 3
    ALBUMS_WORKERS = 5
    TRACKS_WORKERS = 10
    MAX_ALBUMS = None
    MAX_RETRIES = 5
    logger.info("Запуск Spotify API Fetcher")
    try:
        client = SpotifyClient()
        logger.success("Spotify API клиент инициализирован")
    except Exception as e:
        logger.exception(f"Ошибка инициализации API клиента: {e}")
        return
    try:
        with open("artists.csv", "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader)
            except StopIteration:
                logger.error("Файл artists.csv пуст!")
                return
            artist_names = [row[1] for row in reader if len(row) > 1 and row[1].strip()]
    except FileNotFoundError:
        logger.error("Файл artists.csv не найден!")
        return
    except Exception as e:
        logger.error(f"Ошибка чтения artists.csv: {e}")
        return
    if not artist_names:
        logger.error("Не найдено артистов для обработки.")
        return

    logger.info(f"Найдено {len(artist_names)} артистов")
    logger.info(f"Примеры: {', '.join(artist_names[:5])}{'...' if len(artist_names) > 5 else ''}")
    logger.info(f"Артистов параллельно: {ARTISTS_WORKERS}")
    logger.info(f"Альбомов параллельно: {ALBUMS_WORKERS}")
    logger.info(f"Треков параллельно: {TRACKS_WORKERS}")
    logger.info(f"Попыток на артиста: {MAX_RETRIES}")
    logger.info(f"Макс. альбомов: {MAX_ALBUMS if MAX_ALBUMS else 'Все'}")
    filename = "spotify_data.csv"
    start_time = time.time()
    successful_artists = []
    failed_artists = []

    if ARTISTS_WORKERS == 1:
        for artist_name in artist_names:
            logger.info(f"Обработка: {artist_name}")
            success = process_artist_retry(
                csv_lock,
                client,
                artist_name,
                filename,
                ALBUMS_WORKERS,
                TRACKS_WORKERS,
                MAX_ALBUMS,
                MAX_RETRIES)
            if success:
                successful_artists.append(artist_name)
                logger.success(f"Завершён: {artist_name}")
            else:
                failed_artists.append(artist_name)
    else:
        with ThreadPoolExecutor(max_workers=ARTISTS_WORKERS) as executor:
            future_to_artist = {
                executor.submit(
                    process_artist_retry,
                    csv_lock,
                    client,
                    artist_name,
                    filename,
                    ALBUMS_WORKERS,
                    TRACKS_WORKERS,
                    MAX_ALBUMS,
                    MAX_RETRIES
                ): artist_name
                for artist_name in artist_names}

            for future in as_completed(future_to_artist):
                artist_name = future_to_artist[future]
                try:
                    success = future.result()
                    if success:
                        successful_artists.append(artist_name)
                        logger.success(f"Завершён: {artist_name}")
                    else:
                        failed_artists.append(artist_name)
                except Exception as e:
                    failed_artists.append(artist_name)
                    logger.error(f"Критическая ошибка для {artist_name}: {e}")

    elapsed_time = time.time() - start_time
    logger.info(f"Время: {elapsed_time:.2f} секунд")
    logger.info(f"Данные в: {filename}")
    logger.info(f"Всего артистов: {len(artist_names)}")
    logger.success(f"Успешно: {len(successful_artists)}")

    if failed_artists:
        logger.error(f"Не удалось: {len(failed_artists)}")
        logger.error(f"Список: {', '.join(failed_artists)}")
        with open("failed_artists.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(failed_artists))
        logger.warning(f"Необработанные артисты в failed_artists.txt")
    else:
        logger.success(f"Все артисты успешно обработаны!")


if __name__ == "__main__":
    main()
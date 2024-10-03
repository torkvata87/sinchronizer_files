import os
from datetime import datetime, timezone
from math import ceil
from time import time

import ntplib
import requests
from requests import RequestException

from config.logging_config import log


def get_file_path(folder_path: str, file_name: str) -> str:
    """
    Формирует полный путь к файлу на основе имени файла и пути к папке.

    Args:
        folder_path (str): Путь к папке.
        file_name (str): Имя файла.

    Returns:
        str: Полный путь к файлу.
    """

    return os.path.join(folder_path, file_name)


def upload_file(upload_url: str, file_path: str) -> None:
    """
    Загружает файл на указанный URL.

    Args:
        upload_url (str): URL для загрузки файла.
        file_path (str): Путь к файлу для загрузки.

    Raises:
        RequestException: Если произошла ошибка при загрузке файла.
    """

    try:
        with open(file_path, "rb") as file_data:
            response = requests.put(upload_url, file_data)
        response.raise_for_status()
    except RequestException as exc:
        raise RequestException(f"Ошибка при загрузке файла {file_path}: {exc}")


def download_file(download_url: str, file_path: str) -> None:
    """
    Скачивает файл с указанного URL и сохраняет его по указанному пути.

    Args:
        download_url (str): URL для скачивания файла.
        file_path (str): Путь для сохранения скачанного файла.

    Raises:
        RequestException: Если произошла ошибка при скачивании файла.
    """
    try:
        response = requests.get(download_url)
        response.raise_for_status()
        with open(file_path, "wb") as file_data:
            file_data.write(response.content)
    except RequestException as exc:
        raise RequestException(f"Ошибка при скачивании файла {file_path}: {exc}")


def to_unix_timestamp(time_str: str, has_timezone: bool = False) -> int:
    """
    Преобразует строку времени в Unix timestamp.

    Args:
        time_str (str): Строка времени в формате ISO 8601.
        has_timezone (bool): Указание наличия часового пояса.

    Returns:
        int: Время в формате Unix timestamp.
    """

    if has_timezone:
        dt = datetime.fromisoformat(time_str)
        return int(dt.astimezone(timezone.utc).timestamp())
    else:
        dt = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
        return int(dt.timestamp())


def get_ntp_time() -> int:
    """
    Возвращает разницу во времени между локальным и серверным.

    Returns:
         int: Разница во времени между локальным и серверным.

    Raises:
        Exception: Если при запросе к серверу для получения NTP времени возникла ошибка.
    """
    try:
        client = ntplib.NTPClient()
        response = client.request("pool.ntp.org")
        delta_time = ceil(response.tx_time - time())
        ntp_time = datetime.fromtimestamp(response.tx_time)
        log.info(
            f"Текущее синхронизированное NTP время: {ntp_time.strftime('%H:%M:%S')}."
        )
        log.info(
            f"Разница во времени между системным и серверным составляет {delta_time} секунд."
        )
        return delta_time
    except Exception as exc:
        log.error(
            f"Не удалось получить NTP время. Ошибка соединения. {exc}",
        )
        return 0


def get_time_correlation(current_time: int | float, sync_time: int) -> int:
    """
    Возвращает коррелированное время.

    Args:
        current_time (int | float): Текущее время в формате Unix timestamp.
        sync_time (int):  Время синхронизации для корректировки временных меток файлов.

    Returns:
        int: Коррелированное время.
    """
    return ceil(current_time) + sync_time

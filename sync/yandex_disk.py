from config.settings import settings
import requests
from requests.exceptions import ConnectionError, HTTPError
import os

from utils.exceptions import DiskNotFoundError, UnauthorizedError
from config.logging_config import log
from utils.utils import upload_file, download_file, to_unix_timestamp


class ManagerYandexDiskStorage:
    """
    Класс для управления файлами в директории в облаке Яндекс.Диска.

    Attributes:
        _url (str): URL для работы с API Яндекс.Диска.
        _headers (dict[str, str]): Заголовки для авторизации запросов на Яндекс.Диск.
        _backup_folder (str): Путь к папке в облаке, где хранятся файлы.

    Methods:
        backup_folder: Геттер и сеттер для пути к текущей папке резервного копирования.
        load (file_path: str): Загружает файл в облачное хранилище.
        reload (file_path: str): Перезаписывает файл в облаке.
        download (local_folder_path: str, file_name: str): Скачивает файл из облачного хранилища.
        update (file_path: str): Обновляет локальный файл, основываясь на облачной версии.
        delete (file_name: str): Удаляет файл из облачного хранилища.
        get_info: Возвращает информацию о файлах в облаке с указанием времени их изменения.
    """

    def __init__(self, token: str, backup_folder: str):
        """
        Инициализация ManagerYandexDiskStorage.

        Args:
            token (str): OAuth-токен для авторизации в Яндекс.Диске.
            backup_folder (str): Путь к папке резервного копирования в облачном хранилище.
        """

        self._url = "https://cloud-api.yandex.net/v1/disk/resources"
        self._headers = {"Authorization": f"OAuth {token}"}
        self._create_backup_folder(backup_folder)
        self._backup_folder = backup_folder

    @property
    def backup_folder(self) -> str:
        """
        Возвращает путь к текущей папке резервного копирования.

        Returns:
            str: Путь к текущей папке резервного копирования.
        """

        return self._backup_folder

    @backup_folder.setter
    def backup_folder(self, backup_folder) -> None:
        """
        Устанавливает путь к локальной директории.

        Args:
            backup_folder (str): Новый путь к папке для резервного копирования на Яндекс.Диске.
        """

        self._create_backup_folder(backup_folder)
        self._backup_folder = backup_folder

    def _create_backup_folder(self, backup_folder: str) -> None:
        """
        Создает папку резервного копирования на Яндекс.Диске, если она не существует.

        Args:
            backup_folder (str): Путь к создаваемой папке резервного копирования.

        Raises:
            ConnectionError: Если произошла ошибка соединения.
            HTTPError: Если произошла ошибка авторизации.
            Exception: При непредвиденной ошибке.
        """
        params = {"path": backup_folder}
        try:
            check_response = requests.get(
                self._url, headers=self._headers, params=params
            )
            if check_response.status_code == 200:
                return

            response = requests.put(self._url, headers=self._headers, params=params)

            if response.status_code == 201:
                log.info(
                    f"На Яндекс.Диске создана папка {backup_folder.split('/')[-1]} "
                    "для резервного копирования."
                )
            else:
                response.raise_for_status()

        except ConnectionError:
            return
        except HTTPError:
            return
        except Exception as exc:
            log.error(
                f"Ошибка при создании папки {backup_folder} {type(exc).__name__}: {exc}"
            )
            raise

    def _get_transfer_url(self, file_name: str, end_point: str) -> str | None:
        """
        Возвращает URL для получения ссылки местонахождения файла в Яндекс Диске.

        Args:
            file_name (str): Имя файла.
            end_point (str): Конечная точка для загрузки или скачивания ('upload' или 'download').

        Returns:
            str: URL для загрузки/скачивания файла.

        Raises:
            ConnectionError: Если при обращении к серверу возникла ошибка соединения.
            KeyError: Если при получении URL для загрузки/скачивания файла сервер возвращает сообщение об ошибке.
            Exception: При непредвиденной ошибке.
        """

        url = f"{self._url}/{end_point}"
        params = {"path": f"{self.backup_folder}/{file_name}", "overwrite": "true"}
        try:
            response = requests.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            data = response.json()

            if "href" in data:
                return response.json().get("href")
            else:
                raise KeyError(
                    f"Не удалось получить  URL для файла: {data.get('message', 'Неизвестная ошибка')}"
                )
        except ConnectionError:
            raise ConnectionError("Ошибка соединения")
        except Exception as exc:
            raise Exception(
                f"Ошибка при получения URL для файла {file_name} {type(exc).__name__}: {exc}"
            )

    def _get_info_backup_folder(self) -> list[dict[str, str]] | None:
        """
        Возвращает первичную информацию о содержимом папки резервного копирования.

        Returns:
            list[dict[str, str]] | None: Список с информацией о файлах в облачной папке либо None при возникновении
                ошибки.

        Raises:
            ConnectionError: Если при получении информации о файлах возникла ошибка соединения.
            KeyError: Если при получении информации о файлах сервер возвращает сообщение об ошибке.
            UnauthorizedError: Если произошла ошибка авторизации.
            DiskNotFoundError: Если в облаке не найдена папка для синхронизации файлов.
            Exception: При непредвиденной ошибке.
        """

        fields = ",".join(
            [
                "_embedded.items.modified",
                "_embedded.items.name",
                # "_embedded.items.path",
                "_embedded.items.type",
            ]
        )
        params = {"path": self.backup_folder, "fields": fields}

        try:
            response = requests.get(self._url, headers=self._headers, params=params)

            data = response.json()
            try:
                return data["_embedded"]["items"]
            except KeyError:
                if data.get("error") == "DiskNotFoundError":
                    raise DiskNotFoundError(data["message"])
                if data.get("error") == "UnauthorizedError":
                    raise UnauthorizedError
                raise KeyError(
                    f'Ошибка {data.get("error")}: {data.get("message", "Ошибка при получении информации о папке.")}'
                )
        except ConnectionError:
            raise ConnectionError("Ошибка соединения.")
        except UnauthorizedError as exc:
            raise UnauthorizedError(exc)
        except DiskNotFoundError as exc:
            raise DiskNotFoundError(exc)
        except Exception as exc:
            log.error(
                f"Ошибка при получении информации о папке {self.backup_folder}: {exc}"
            )
            return None

    def load(
        self, local_folder_path: str, file_name: str, is_load: bool = True
    ) -> None:
        """Загружает файл в облачное хранилище.

        Args:
            local_folder_path (str): Путь к локальной папке для сохранения файла.
            file_name (str): Имя файла в облаке.
            is_load (bool): Флаг, фиксирующий, используется ли функция для загрузки нового файла в облако.
                По умолчанию True.

        Raises:
            OSError: Если локальная директория по указанному пути не найдена.
            FileNotFoundError: Если файл не найден в локальной директории.
            Exception: При непредвиденной ошибке.
        """

        add_text = "" if is_load else "пере"
        if not os.path.isdir(local_folder_path):
            raise OSError(
                f"Указанного пути к локальному хранилищу {local_folder_path} не существует."
            )

        local_file_path = os.path.join(local_folder_path, file_name)
        try:
            load_url = self._get_transfer_url(file_name, end_point="upload")
            upload_file(load_url, local_file_path)
            if is_load:
                log.info(f"Файл {file_name} успешно записан.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Файл {file_name} не найден.")
        except Exception as exc:
            raise Exception(f"Файл {file_name} не {add_text}записан. {exc}")

    def reload(self, local_folder_path: str, file_name: str) -> None:
        """Перезаписывает файл в облачном хранилище.

        Args:
            local_folder_path (str): Путь к локальной папке для сохранения файла.
            file_name (str): Имя файла в облаке.
        """

        local_file_path = os.path.join(local_folder_path, file_name)

        if not os.path.exists(local_file_path):
            log.info(
                f"Файл {file_name} не найден в {local_folder_path}, создан новый файл."
            )
        self.load(local_folder_path, file_name, is_load=False)
        log.info(f"Файл {file_name} успешно перезаписан.")

    def download(
        self, local_folder_path: str, file_name: str, is_download: bool = True
    ) -> None:
        """
        Скачивает файл из облачного хранилища в локальное.

        Args:
            local_folder_path (str): Путь к локальной папке для сохранения файла.
            file_name (str): Имя файла в облаке.
            is_download: Флаг, фиксирующий, используется ли функция для скачивания нового файла из облака.
                По умолчанию True.

        Raises:
            OSError: Если локальная директория по указанному пути не найдена.
            HTTPError: Если возникла ошибка при загрузке файла в облачное хранилище.
            Exception: При непредвиденной ошибке.
        """

        local_file_path = os.path.join(local_folder_path, file_name)

        if not os.path.isdir(local_folder_path):
            raise OSError(
                f"Указанного пути к локальному хранилищу {local_folder_path} не существует."
            )
        try:
            download_url = self._get_transfer_url(file_name, end_point="download")
            download_file(download_url, local_file_path)
            if is_download:
                log.info(f"Файл {file_name} успешно скачан.")
        except HTTPError as exc:
            log.error(f"Файл {file_name} не скачан. Ошибка {type(exc).__name__}.")
        except Exception as exc:
            raise Exception(f"Файл {file_name} не скачан. Ошибка {type(exc).__name__}.")

    def update(self, local_folder_path: str, file_name: str) -> None:
        """
        Обновляет файл, измененный в облачном хранилище, в локальном.

        Args:
            local_folder_path (str): Путь к локальной папке для обновления файла.
            file_name (str): Имя файла в облаке.
        """

        local_file_path = os.path.join(local_folder_path, file_name)

        if not os.path.exists(local_file_path):
            log.error(f"Файл {file_name} не найден в {local_folder_path}")

        self.download(local_folder_path, file_name, is_download=False)
        log.info(f"Измененный в облаке файл {file_name} успешно перезаписан.")

    def delete(self, file_name: str):
        """
        Удаляет файл из облачного хранилища.

        Args:
            file_name (str): Имя файла для удаления.

        Raises:
            KeyError: Если файл не найден в облачном хранилище.
            ConnectionError: Если при удалении файла возникла ошибка соединения.
            Exception: При непредвиденной ошибке.
        """

        try:
            if file_name not in self.get_info():
                log.info(
                    f"Файл {file_name} не найден в папке {self.backup_folder}. Удаление не требуется."
                )
                return
            params = {"path": f"{self.backup_folder}/{file_name}"}
            response = requests.delete(self._url, headers=self._headers, params=params)
            response.raise_for_status()
            log.info(f"Файл {file_name} успешно удален.")
            return response
        except KeyError as exc:
            log.error(f"{type(exc).__name__}: {exc}")
        except ConnectionError:
            raise ConnectionError(
                f"Файл {file_name} в облаке не удален. Ошибка соединения"
            )
        except Exception as exc:
            raise Exception(
                f"Файл {file_name} в облаке не удален. Ошибка {type(exc).__name__}"
            )

    def get_info(self) -> dict[str, int]:
        """
        Возвращает информацию о файлах в облачном хранилище.

        Returns:
            dict[str, int]: Информация о файлах в облачном хранилище в виде словаря.

        Raises:
            KeyError: Если при получении информации о файлах сервер вернул сообщение об ошибке.
            DiskNotFoundError: Если в облаке не найдено хранилище для синхронизации файлов.
        """

        try:
            list_files_cloud = self._get_info_backup_folder()
        except DiskNotFoundError:
            self._create_backup_folder(self._backup_folder)
            raise DiskNotFoundError
        except KeyError:
            return {}

        if not list_files_cloud:
            return {}

        cloud_files_info = {}

        for item in list_files_cloud:
            if item["name"].startswith("~") or item["type"] != "file":
                log.info(
                    f"{item["name"]} в облаке является {item["type"]} и не синхронизируется."
                )
            else:
                cloud_files_info[item["name"]] = to_unix_timestamp(
                    item["modified"], has_timezone=True
                )
        return cloud_files_info


if __name__ == "__main__":
    manager_cloud = ManagerYandexDiskStorage(
        settings.yandex_disk_token.get_secret_value(),
        settings.name_folder_in_cloud_storage,
    )
    print(manager_cloud.get_info())

from time import time

from requests.exceptions import ConnectionError

from utils.exceptions import DiskNotFoundError, UnauthorizedError
from sync.metadata_manager import MetadataCache
from utils.utils import get_time_correlation
from sync.yandex_disk import ManagerYandexDiskStorage
from config.logging_config import log
from sync.local_storage import ManagerLocalStorage


class StorageSynchronizer:
    """
    Класс для синхронизации файлов между локальной папкой и облаком Яндекс.Диска.

    Attributes:
        _manager_local (ManagerLocalStorage): Объект класса ManagerLocalStorage.
        _manager_cloud (ManagerYandexDiskStorage): Объект класса ManagerYandexDiskStorage.
        _cache (MetadataCache): Объект класса MetadataCache.
        _local_info (dict[str, int] | None): Информация о файлах в локальном хранилище.
        _cloud_info (dict[str, int] | None): Информация о файлах в облаке.
        _sync_time (int): Время синхронизации для корректировки временных меток файлов.

    Methods:
        synchronize_data: Выполняет полную синхронизацию файлов.
    """

    def __init__(
        self,
        manager_local: ManagerLocalStorage,
        manager_cloud: ManagerYandexDiskStorage,
        metadata_cache: MetadataCache,
        sync_time: int,
    ) -> None:
        """
        Инициализация StorageSynchronizer.

        Args:
            manager_local (ManagerLocalStorage): Объект класса ManagerLocalStorage.
            manager_cloud (ManagerYandexDiskStorage): Объект класса ManagerYandexDiskStorage.
            metadata_cache (MetadataCache): Объект класса MetadataCache.
        """

        self._manager_local = manager_local
        self._manager_cloud = manager_cloud
        self._cache = metadata_cache
        self._local_info = {}
        self._cloud_info = {}
        self._sync_time = sync_time

    def _update_data(self, file_name: str, storage_info: dict[str, int]) -> None:
        """
        Обновляет данные кеша и информации о файлах локального или облачного хранилища.

        Args:
            file_name (str): Имя файла.
            storage_info (dict[str, int]): Информация о файлах в локальном или облачном хранилище.

        Raises:
            Exception: Если при обновлении файла возникла ошибка.
        """

        try:
            current_time = time()
            time_change = get_time_correlation(current_time, self._sync_time)
            storage_info[file_name] = time_change
            self._cache.update_file_cache(file_name, time_change)
        except Exception as exc:
            raise Exception(
                f"Ошибка при обновлении данных для файла {file_name}: {exc}"
            )

    def _delete_data(self, file_name: str, storage_info: dict[str, int]) -> None:
        """
        Удаляет данные из кеша и из информации о файлах локального или облачного хранилища.

        Args:
            file_name (str): Имя файла.
            storage_info (dict[str, int]): Информация о файлах в локальном или облачном хранилище.
        """

        if file_name in storage_info:
            storage_info.pop(file_name)
        self._cache.delete_file_cache(file_name)

    def _transfer_to_storage(
        self, file_name: str, storage_info: dict[str, int], func_transfer: callable
    ) -> None:
        """
        Если файла нет в кеше, отправляет файл на загрузку в облако или на скачивание из облака с обновлением данных.

        Args:
            file_name (str): Имя файла.
            storage_info (dict[str, int]): Информация о файлах в локальном или облачном хранилище.
            func_transfer (callable): Функция для загрузки или скачивания файла.

        Raises:
            Exception: Если при передаче файла возникла ошибка.
        """

        try:
            if file_name not in self._cache.metadata:
                func_transfer(self._manager_local.path_local_folder, file_name)
                self._update_data(file_name, storage_info)
        except Exception as exc:
            log.error(f"{exc}")

    def _reload_to_storage(
        self,
        file_name: str,
        mod_time: int,
        storage_info: dict[str, int],
        reload_func: callable,
    ) -> None:
        """
        Если время последнего изменения файла больше, чем в кеше, перезаписывает файл в локальном хранилище или
        в облаке с обновлением данных.

        Args:
            file_name (str): Имя файла.
            mod_time (int): Время последнего изменения файла.
            storage_info (dict[str, int]): Информация о файлах в локальном или облачном хранилище.
            reload_func (callable): Функция для перезаписи файла.

        Raises:
            TypeError: Если при ошибке перезаписи файла время возвращается как None.
            Exception: Если при перезаписи файла возникла ошибка.
        """

        try:
            if mod_time > self._cache.get_mod_time(file_name):
                reload_func(self._manager_local.path_local_folder, file_name)
                self._update_data(file_name, storage_info)
        except TypeError:
            return
        except Exception as exc:
            log.error(exc)

    def _delete_in_storage(
        self,
        manager: ManagerLocalStorage | ManagerYandexDiskStorage,
        storage_info: dict[str, int],
        is_first_launch: bool = False,
    ) -> None:
        """
        Если в кеше отсутствует файл, удаляет файл из локального хранилища или из облака с обновлением данных.

        Args:
            manager (ManagerLocalStorage | ManagerYandexDiskStorage): Менеджер хранилища.
            storage_info (dict[str, int]): Информация о файлах в локальном или облачном хранилище.
            is_first_launch (bool): Флаг для выполнения первого запуска синхронизации файлов. По умолчанию False.

        Raises:
            FileNotFoundError: Если при удалении файла он не найден в локальном хранилище.
            Exception: Если при удалении файла возникла ошибка.
        """

        storage_info_other = (
            self._local_info if storage_info == self._cloud_info else self._cloud_info
        )

        if not storage_info and storage_info == self._local_info and is_first_launch:
            self._cache.delete_data_cache()

        for file_name in list(self._cache.metadata):
            try:
                if file_name not in storage_info and not is_first_launch:
                    try:
                        manager.delete(file_name)
                        self._transfer_to_storage(
                            file_name, self._local_info, self._manager_cloud.load
                        )
                        self._delete_data(file_name, storage_info_other)
                    except FileNotFoundError as exc:
                        log.error(exc)
                        self._cache.delete_file_cache(file_name)
                        log.info(f"{file_name} удален из кеша.")
                    except Exception as exc:
                        log.error(exc)

                if file_name not in storage_info and is_first_launch:
                    if storage_info == self._cloud_info:
                        self._manager_cloud.load(
                            self._manager_local.path_local_folder, file_name
                        )
                        self._update_data(file_name, storage_info)

            except FileNotFoundError as exc:
                self._cache.delete_file_cache(file_name)
                log.error(exc)
            except Exception as exc:
                log.error(exc)

    def _sync_files_change_locally(self) -> None:
        """
        Синхронизирует файлы в локальном хранилище с облаком.

        Raises:
            Exception: Если при синхронизации локальных изменений возникла ошибка.
        """

        try:
            for file_name, local_mod_time in self._local_info.items():
                self._transfer_to_storage(
                    file_name, self._cloud_info, self._manager_cloud.load
                )

                self._reload_to_storage(
                    file_name,
                    local_mod_time,
                    self._cloud_info,
                    self._manager_cloud.reload,
                )
        except Exception as exc:
            log.error(exc)

    def _sync_files_change_cloudy(self) -> None:
        """
        Синхронизирует файлы в облаке с локальным хранилищем.

        Raises:
            Exception: Если при синхронизации изменений в облаке возникла ошибка.
        """

        try:
            for file_name, cloud_mod_time in self._cloud_info.items():
                self._transfer_to_storage(
                    file_name, self._local_info, self._manager_cloud.download
                )

                self._reload_to_storage(
                    file_name,
                    cloud_mod_time,
                    self._local_info,
                    self._manager_cloud.update,
                )
        except Exception as exc:
            log.error(f"Ошибка при синхронизации изменений в облаке: {exc}")

    def synchronize_data(self, is_first_launch: bool = False) -> None:
        """
        Выполняет полную синхронизацию файлов.

        Args:
            is_first_launch (bool): Флаг для выполнения первого запуска синхронизации файлов. По умолчанию False.

        Raises:
            ConnectionError: Если при синхронизации файлов возникла ошибка соединения.
            UnauthorizedError: Если при синхронизации файлов возникла ошибка авторизации.
            OSError: Если при синхронизации файлов возникла ошибка доступа к локальному хранилищу.
            DiskNotFoundError: Если при синхронизации файлов возникла ошибка доступа к облачному хранилищу.
            Exception: Если при синхронизации файлов возникла непредвиденная ошибка.
        """

        log.info(
            f"Программа синхронизации файлов начинает работу с директорией {self._manager_local.path_local_folder}."
        )
        try:
            self._local_info = self._manager_local.get_info()

            if not self._cache.metadata:
                self._cache.metadata = self._local_info

            self._sync_files_change_locally()
            self._delete_in_storage(
                self._manager_cloud, self._local_info, is_first_launch
            )

            if is_first_launch:
                self._cloud_info = self._manager_cloud.get_info()
                self._sync_files_change_cloudy()
                self._delete_in_storage(
                    self._manager_local, self._cloud_info, is_first_launch
                )
            log.info("Синхронизация файлов завершена.")
        except ConnectionError:
            log.error(f"Неудачная попытка синхронизации файлов. Ошибка соединения.")
        except UnauthorizedError as exc:
            log.error(f"Неудачная попытка синхронизации файлов. {exc}")
        except (OSError, DiskNotFoundError) as exc:
            log.error(exc)
            log.info(
                "Для устранения ошибки доступа к директории выполняется перезапуск синхронизации файлов..."
            )
            self.synchronize_data(is_first_launch=True)
        except Exception as exc:
            log.error(
                f"Неудачная попытка синхронизации файлов. Ошибка {type(exc).__name__}: {exc}"
            )

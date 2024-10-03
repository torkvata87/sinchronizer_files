import os
from time import sleep

from config.settings import settings
from sync.metadata_manager import MetadataCache
from sync.local_storage import ManagerLocalStorage
from sync.sync_data import StorageSynchronizer
from config.logging_config import log
from utils.utils import get_ntp_time
from sync.yandex_disk import ManagerYandexDiskStorage


def launch_file_synchronizer(
    token_user: str,
    path_local_folder_user: str,
    backup_folder_user: str,
    sync_period: int,
    path_cache: str = "metadata_local_cache.json",
) -> None:
    """
    Главная функция для запуска синхронизации данных между локальным и облачным хранилищем.

    Функция инициализирует менеджеры для локального и облачного хранилищ, кэш метаданных,
    а затем запускает бесконечный цикл синхронизации с заданным интервалом.
    В случае ошибки при синхронизации файлы продолжают обрабатываться без остановки цикла.

    Args:
        token_user (str): Токен доступа к облачному хранилищу Яндекс.Диска.
        path_local_folder_user (str): Путь к синхронизируемой папке.
        backup_folder_user (str): Имя папки в облачном хранилище.
        sync_period (int): Период синхронизации.
        path_cache (str): Путь к файлу кеша. По умолчанию "metadata_local_cache.json".

    Raises:
        Exception: Если при синхронизации файлов возникла ошибка.
    """
    project_root = os.path.abspath(os.path.dirname(__file__))
    if not os.path.isabs(path_cache):
        path_cache = os.path.join(os.path.join(project_root, path_cache))

    sync_time = get_ntp_time()

    manager_local = ManagerLocalStorage(path_local_folder_user, sync_time)
    manager_cloud = ManagerYandexDiskStorage(token_user, backup_folder_user)
    manager_local_cache = MetadataCache(path_cache)

    synchronizer = StorageSynchronizer(
        manager_local, manager_cloud, manager_local_cache, sync_time
    )

    synchronizer.synchronize_data(is_first_launch=True)

    while True:
        sleep(sync_period)
        try:
            synchronizer.synchronize_data()
        except Exception as exc:
            log.error(f"Ошибка при синхронизации файлов {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    token = settings.yandex_disk_token.get_secret_value()
    path_local_folder = settings.path_folder
    backup_folder = settings.name_folder_in_cloud_storage
    synchronization_period = settings.synchronization_period
    path_cache = "metadata_cache.json"

    launch_file_synchronizer(
        token, path_local_folder, backup_folder, synchronization_period, path_cache
    )

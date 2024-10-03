import math
import os
from datetime import datetime, timezone

from config.settings import settings
from utils.utils import get_file_path, get_time_correlation
from config.logging_config import log


class ManagerLocalStorage:
    """
    Класс для управления файлами в локальной директории.

    Attributes:
        _path_local_folder (str): Путь к локальной папке, с которой работает класс.
        _sync_time (int): Время синхронизации для корректировки временных меток файлов.

    Methods:
        path_local_folder: Геттер и сеттер для пути к локальной директории.
        get_info: Возвращает информацию о файлах и времени их последнего изменения в локальной директории.
        delete(file_name): Удаляет файл из локальной директории.
    """

    def __init__(self, path_local_folder: str, sync_time: int) -> None:
        """
        Инициализация ManagerLocalStorage.

        Args:
            path_local_folder (str): Путь к локальной директории.
            sync_time (int): Время синхронизации для корректировки временных меток файлов.
        """

        self._path_local_folder = path_local_folder
        self._sync_time = sync_time
        self._ensure_local_folder_exists()

    def _ensure_local_folder_exists(self) -> None:
        """
        Проверяет наличие локальной директории и создаёт её, если она отсутствует.
        Если путь указан неверно или пуст, устанавливает путь по умолчанию и создаёт папку.

        Raises:
            OSError: Если возникла ошибка при создании локальной директории. Создается локальная директория
                по умолчанию.
        """

        if not self._path_local_folder or not os.path.isdir(self._path_local_folder):
            default_folder = "local_folder_sync"
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), os.path.pardir)
            )

            if not self._path_local_folder:
                self._path_local_folder = os.path.join(project_root, default_folder)
                log.error(
                    "Путь к локальной директории не задан. "
                    f"Создана папка по умолчанию: {self._path_local_folder}"
                )

            try:
                os.makedirs(self._path_local_folder, exist_ok=True)
                log.info(
                    f"Создана папка для локального хранения данных: {self._path_local_folder}"
                )
            except OSError as exc:
                self._path_local_folder = os.path.join(project_root, default_folder)
                os.makedirs(self._path_local_folder, exist_ok=True)
                log.error(f"Ошибка при создании директории: {exc}. ")
                log.info(f"Создана папка по умолчанию: {self._path_local_folder}")

    @property
    def path_local_folder(self) -> str:
        """
        Возвращает путь к локальной директории.

        Returns:
            str: Путь к локальной директории.
        """
        return self._path_local_folder

    @path_local_folder.setter
    def path_local_folder(self, path_local_folder: str) -> None:
        """
        Устанавливает путь к локальной директории.

        Args:
            path_local_folder (str): Новый путь к локальной директории.
        """

        self._path_local_folder = path_local_folder
        self._ensure_local_folder_exists()

    def _get_file_time_modified(self, file_name: str) -> int | None:
        """
        Возвращает время последнего изменения файла в формате Unix timestamp.

         Args:
            file_name (str): Имя файла в локальной директории.

        Returns:
            int: Время последнего изменения файла в формате Unix timestamp с округлением в большую сторону
                (с использованием функции math.ceil).

        Raises:
            FileNotFoundError: Если файл не найден по указанному пути.
            OSError: Если возникла другая ошибка при доступе к файлу.
        """
        try:
            file_path = os.path.join(self._path_local_folder, file_name)
            mod_time = os.path.getmtime(file_path)
            local_time = datetime.fromtimestamp(mod_time, tz=timezone.utc)
            return math.ceil(local_time.timestamp())
        except FileNotFoundError as exc:
            log.error(f"Файл {file_name} не найден: {exc}")
        except OSError as exc:
            log.error(f"Ошибка доступа к файлу {file_name}: {exc}")

    def delete(self, file_name: str) -> None:
        """
        Удаляет файл из локальной директории.

        Args:
            file_name (str): Имя файла, который нужно удалить.

        Raises:
            FileNotFoundError: Если файл не найден.
            OSError: Если возникла ошибка доступа к файлу.
            Exception: Если возникает непредвиденная ошибка.
        """
        try:
            file_path = get_file_path(self._path_local_folder, file_name)
            os.remove(file_path)
            log.info(f"Локальный файл {file_name} успешно удален.")
        except FileNotFoundError:
            # log.error(f"Локальный файл {file_name} не найден.")
            raise FileNotFoundError(f"Локальный файл {file_name} не найден.")

        except OSError as exc:
            log.error(f"Ошибка доступа к файлу {file_name}: {exc}")
            raise OSError(exc)

        except Exception as exc:
            raise Exception(
                f"Ошибка при удалении локального файла {file_name} {type(exc).__name__}: {exc}"
            )

    def get_info(self) -> dict[str, int] | None:
        """
        Возвращает информацию о файлах в локальной директории с указанием времени последнего их изменения.
        Файлы, начинающиеся с символа "~", игнорируются.

        Returns:
            dict[str, int]: Словарь, где ключ — имя файла, значение — время последнего изменения в формате
                Unix timestamp.

        Raises:
            FileNotFoundError: Если файл не найден по указанному пути.
            OSError: Если возникла другая ошибка при доступе к файлу.
        """
        try:
            local_files = {}
            for file_name in os.listdir(self._path_local_folder):
                file_path = os.path.join(self._path_local_folder, file_name)
                if os.path.isfile(file_path) and not file_name.startswith("~"):
                    try:
                        time_modified = self._get_file_time_modified(file_name)
                        local_files[file_name] = get_time_correlation(
                            time_modified, self._sync_time
                        )
                    except FileNotFoundError:
                        log.error(
                            f"Файл {file_name} не найден в директории {self._path_local_folder}"
                        )
                    except OSError as exc:
                        log.error(f"Ошибка при доступе к файлу {file_name}: {exc}")
                else:
                    log.info(
                        f"{file_name} является недопустимым файлом или директорией."
                    )
        except OSError:
            os.makedirs(self._path_local_folder, exist_ok=True)

            raise OSError(
                f"Ошибка доступа к локальной директории {self._path_local_folder}"
            )

        return local_files


if __name__ == "__main__":
    local_manager = ManagerLocalStorage(settings.path_folder, sync_time=0)
    print(local_manager.get_info())

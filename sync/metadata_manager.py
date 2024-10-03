import json
import os

from config.logging_config import log


class MetadataCache:
    """
    Класс для управления данными кеша.

    Attributes:
        _cache (str): Путь к файлу кеша.
        _metadata (dict[str, int]): Информация о файлах в кеше в виде словаря с именами файлов и временем их изменения.

    Methods:
        cache (str): Геттер и сеттер для пути к файлу кеша.
        metadata (dict[str, int]): Геттер и сеттер для информации о файлах в кеше.
        update_file_cache (str, int): Обновляет информацию о файле в кеше.
        delete_file_cache (str): Удаляет информацию о файле из кеша.
        get_mod_time (str) -> int: Возвращает время последнего изменения файла из кеша.
    """

    def __init__(self, cache: str) -> None:
        """
        Инициализация MetadataCache.

        Args:
            cache (str): Путь к файлу кеша.
        """

        self._cache = cache
        self._ensure_cache_file_exists()
        self._metadata = self._load_cache()

    @property
    def cache(self) -> str:
        """
        Возвращает путь к файлу кеша.

        Returns:
            str: Путь к файлу кеша.
        """

        return self._cache

    @cache.setter
    def cache(self, cache: str) -> None:
        """
        Устанавливает новую информацию о файлах в кеше и записывает её в файл.

        Args:
            cache (str): Новый путь к файлу кеша.
        """

        self._cache = cache
        self._ensure_cache_file_exists()

    @property
    def metadata(self) -> dict[str, int]:
        """
        Возвращает информацию о файлах в кеше.

        Returns:
            dict[str, int]: Словарь с именами файлов и временем их последнего изменения.
        """

        return self._metadata

    @metadata.setter
    def metadata(self, metadata: dict[str, int]) -> None:
        """
        Устанавливает новую информацию о файлах в кеше и записывает её в файл.

        Args:
            metadata (dict[str, int]): Новая информация о файлах.
        """

        self._metadata = metadata
        self._dump_cache()

    def _ensure_cache_file_exists(self) -> None:
        """
        Проверяет наличие локальной директории и создаёт её, если она отсутствует.
        Если путь указан неверно, устанавливает значение по умолчанию.
        """

        if not self._cache.endswith(".json"):
            default_cache_file = "metadata_local_cache.json"
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), os.path.pardir)
            )
            self._cache = os.path.join(project_root, default_cache_file)

            log.error("Путь к файлу кеша отсутствует или задан неверно.")
            log.info(f"Задан путь к файлу кеша по умолчанию: {self._cache}")

    def _load_cache(self) -> dict[str, int]:
        """
        Извлекает данные из кеша.

        Returns:
            dict[str, int]: Словарь с данными о файлах в кеше.

        Raises:
            IOError: Если возникла ошибка при чтении кэша.
            json.JSONDecodeError: Если возникла ошибка при сериализации данных в JSON.
        """

        if os.path.exists(self._cache):
            try:
                with open(self._cache, "r", encoding="utf-8") as file_data:
                    return json.load(file_data)
            except (IOError, json.JSONDecodeError) as exc:
                log.error(f"Ошибка при чтении кэша {type(exc).__name__}: {exc}")
                return {}
        return {}

    def _dump_cache(self) -> None:
        """
        Записывает данные в файл кеша.

        Raises:
            IOError: Если возникла ошибка при записи в кэш.
            json.JSONDecodeError: Если возникла ошибка при сериализации данных в JSON.
        """

        try:
            with open(self._cache, "w", encoding="utf-8") as file_data:
                json.dump(self._metadata, file_data, indent=4, ensure_ascii=False)
        except (IOError, json.JSONDecodeError) as exc:
            log.error(f"Ошибка при записи в кэш: {exc}")

    def update_file_cache(self, file_name: str, mod_time: int) -> None:
        """
        Обновляет данные файла в кеше и записывает их в файл.

        Args:
            file_name (str): Имя файла.
            mod_time (int): Время последнего изменения файла.
        """

        self._metadata[file_name] = mod_time
        self._dump_cache()

    def delete_file_cache(self, file_name: str) -> None:
        """
        Удаляет данные о файле из кеша и обновляет файл кеша.

        Args:
            file_name (str): Имя файла для удаления.

        Raises:
            Exception: Если возникла ошибка при обновлении кеша.
        """

        if file_name in self._metadata:
            self._metadata.pop(file_name)
            try:
                self._dump_cache()
            except Exception as exc:
                log.error(
                    f"Ошибка при обновлении кеша после удаления файла '{file_name}': {exc}"
                )
        else:
            log.info(f"Файл '{file_name}' не найден в кеше. Удаление не требуется.")

    def delete_data_cache(self) -> None:
        """
        Полностью очищает кеш.
        """

        self._metadata = {}
        self._dump_cache()

    def get_mod_time(self, file_name) -> int:
        """
        Возвращает время последнего изменения файла из кеша.

        Args:
            file_name (str): Имя файла.

        Returns:
            int: Время последнего изменения файла.
        """

        return self._metadata.get(file_name)

from loguru import logger
import os

from config.settings import settings


def setup_logger(path_log_file: str) -> logger:
    """
    Настраивает логирование с использованием библиотеки loguru.

    Логгер настраивается с параметрами:
        - Уровень логирования (DEBUG для информационных сообщений).
        - Формат сообщений.
        - Ротация логов при достижении 50 МБ.
        - Включение стека вызовов (backtrace) и подробной диагностики (diagnose).

    Args:
        path_log_file (str): Путь к файлу лога.

    Returns:
        logger: Настроенный объект логгера loguru.
    """

    formatting = "syncronizer {time:YYYY-MM-DD HH:mm:ss,SSS} {level} {message}"

    log_dir = os.path.dirname(path_log_file)
    os.makedirs(log_dir, exist_ok=True)

    logger.add(
        settings.path_log_file,
        format=formatting,
        level="DEBUG",
        rotation="50 MB",
        backtrace=True,
        diagnose=True,
    )

    return logger


log = setup_logger(settings.path_log_file)

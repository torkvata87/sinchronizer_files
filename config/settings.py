import os

from dotenv import load_dotenv, find_dotenv
from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings


env_file = find_dotenv()
if not env_file:
    exit(
        "Переменные окружения не загружены: отсутствует файл .env.\n"
        "Для корректной работы создайте файл .env с необходимыми переменными окружения."
    )
else:
    try:
        load_dotenv(env_file)
    except Exception as exc:
        exit(f"Ошибка загрузки .env файла: {exc}")


class Settings(BaseSettings):
    yandex_disk_token: SecretStr
    path_folder: str
    name_folder_in_cloud_storage: str
    synchronization_period: int
    path_log_file: str = "logs/app.log"

    class Config:
        env_file = ".env"

    def __init__(self, **kwargs):
        """
        Инициализирует экземпляр Settings.
        Наследует и вызывает конструктор базового класса BaseSettings.
        """

        super().__init__(**kwargs)
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.path.pardir)
        )
        if self.synchronization_period < 0:
            self.synchronization_period *= -1
        if not os.path.isabs(self.path_folder):
            self.path_folder = os.path.join(
                os.path.join(project_root, self.path_folder)
            )

        if not os.path.isabs(self.path_log_file):
            self.path_log_file = os.path.join(
                os.path.join(project_root, self.path_log_file)
            )


try:
    settings = Settings()
except ValidationError as exc:
    dict_fields = {
        "yandex_disk_token": "YANDEX_DISK_TOKEN",
        "path_folder": "PATH_FOLDER",
        "name_folder_in_cloud_storage": "NAME_FOLDER_IN_CLOUD_STORAGE",
        "synchronization_period": "SYNCHRONIZATION_PERIOD",
        "path_log_file": "PATH_LOG_FILE",
    }

    error_fields = [error.get("loc")[0] for error in exc.errors()]

    missing_variables = [
        dict_fields[error["loc"][0]]
        for error in exc.errors()
        if error["loc"][0] in dict_fields
    ]

    if missing_variables:
        variables_str = ", ".join(missing_variables)
        exit(
            f"Отсутствуют необходимые переменные окружения: {variables_str}.\n"
            "Для корректной работы укажите их в файле .env."
        )

class DiskNotFoundError(Exception):
    """Ошибка, возникающая, если диск или директория в облаке не найдены."""

    def __init__(self, message="Директория в облаке не найдена или недоступна."):
        super().__init__(message)


class UnauthorizedError(Exception):
    """Ошибка, возникающая, если неправильный токен API Яндекс Диска."""

    def __init__(self, message="Неправильный токен API Яндекс Диска."):
        super().__init__(message)

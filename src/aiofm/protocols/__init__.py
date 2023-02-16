from abc import ABCMeta, abstractmethod
from pathlib import PurePath
from typing import Sequence, Tuple


class BaseProtocol(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__()

    @staticmethod
    def _split_path(path: str | PurePath):
        try:
            path_parts = path.parts
        except AttributeError:
            path_parts = PurePath(path).parts

        return path_parts

    @staticmethod
    @abstractmethod
    async def ls(path: str, pattern: str = None, *args, **kwargs) -> Sequence:
        pass

    @abstractmethod
    async def open(self, path, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    async def exists(path: str) -> bool:
        pass

    @staticmethod
    @abstractmethod
    async def cp(src_path: str, dst_path: str):
        pass

    @abstractmethod
    async def mkdir(self, path: str):
        pass

    @abstractmethod
    async def mkdirs(self, path: str):
        pass

    @abstractmethod
    async def mv(self, src_path: str, dst_path: str):
        pass

    @abstractmethod
    async def rm(self, path: str):
        pass

    @staticmethod
    @abstractmethod
    async def is_dir(path: str) -> bool:
        pass

    @abstractmethod
    async def glob(self, pattern: str) -> Tuple:
        pass


def get_protocol_for_path(path: str) -> BaseProtocol:
    raise NotImplemented

import os.path
from abc import ABCMeta, abstractmethod
from typing import Sequence, Tuple


class BaseProtocol(metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.path_sep = os.path.sep

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


class MemProtocol(BaseProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path_sep = os.path.sep

    @staticmethod
    async def ls(path: str, pattern: str = None, *args, **kwargs) -> Sequence:
        pass

    async def open(self, path, *args, **kwargs):
        pass

    @staticmethod
    async def exists(path: str) -> bool:
        pass

    @staticmethod
    async def cp(src_path: str, dst_path: str):
        pass

    async def mkdir(self, path: str):
        pass

    async def mkdirs(self, path: str):
        pass

    async def mv(self, src_path: str, dst_path: str):
        pass

    async def rm(self, path: str):
        pass

    @staticmethod
    async def is_dir(path: str) -> bool:
        pass

    async def glob(self, pattern: str) -> Tuple:
        pass

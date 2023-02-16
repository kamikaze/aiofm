from contextlib import asynccontextmanager


class FileManager:
    @staticmethod
    def cp(src_path: str, dst_path: str):
        raise NotImplemented

    @staticmethod
    def rm(path: str):
        raise NotImplemented

    @staticmethod
    def ls(path: str):
        raise NotImplemented

    @staticmethod
    def mkdir(path: str):
        raise NotImplemented

    @staticmethod
    @asynccontextmanager
    def open(path: str, mode: str = 'r'):
        raise NotImplemented

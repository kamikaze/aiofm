import collections.abc
import logging
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import PurePath
from typing import Sequence, Tuple, AsyncGenerator

import urllib3
from aiobotocore.session import get_session
from botocore.response import StreamingBody
from minio import Minio
from pydantic import SecretStr

from aiofm.protocols import BaseProtocol

logger = logging.getLogger(__name__)


class S3ReadableFile:
    def __init__(self, stream):
        self.stream = stream

    def read(self, size=-1):
        if size == -1:
            for chunk in self.stream.iter_chunks(16777216):  # 16MB
                yield chunk
        else:
            for chunk in self.stream.iter_chunks(size):
                yield chunk

    def __iter__(self):
        yield from self.read()

    def close(self):
        self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class S3WritableFile(BytesIO):
    def __init__(self, bucket_name: str, object_key: str, s3_client):
        super().__init__()
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.s3_client = s3_client
        self.flushed_to_s3 = False

    def write(self, data):
        if isinstance(data, StreamingBody):
            self.s3_client.upload_fileobj(data, self.bucket_name, self.object_key)
        elif isinstance(data, S3ReadableFile):
            self.s3_client.upload_fileobj(data.stream, self.bucket_name, self.object_key)
        elif isinstance(data, bytes):
            super().write(data)
        else:
            raise ValueError(f'Unsupported data type: {type(data)}')

    def close(self):
        if not self.flushed_to_s3 and self.tell() > 0:
            self.flushed_to_s3 = True
            self.seek(0)
            self.s3_client.upload_fileobj(self, Bucket=self.bucket_name, Key=self.object_key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class S3Protocol(BaseProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = get_session().create_client('s3')

    @staticmethod
    def _split_path(path: str | PurePath) -> Sequence[str]:
        try:
            path_parts = path.parts
        except AttributeError:
            path_parts = PurePath(path).parts

        if path_parts[0] == '/':
            bucket_name, prefix_parts = path_parts[1], path_parts[2:]
        else:
            bucket_name, prefix_parts = path_parts[0], path_parts[1:]

        if prefix_parts:
            prefix = '/'.join(prefix_parts)
        else:
            prefix = ''

        return bucket_name, prefix

    async def ls(self, path: str | PurePath, pattern: str = None, *args,
                 **kwargs) -> AsyncGenerator[PurePath, None]:
        bucket_name, prefix = self._split_path(path)
        has_items = False

        async with self.client as client:
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            async for page in page_iterator:
                for item in page.get('Contents', []):
                    has_items = True
                    yield PurePath(f'/{bucket_name}/{item["Key"]}')

        if not has_items:
            raise FileNotFoundError

    def open(self, path: str | PurePath, *args, **kwargs):
        mode = kwargs.pop('mode', args[0] if len(args) else 'r')
        bucket_name, path = self._split_path(path)

        try:
            if 'b' not in mode:
                raise ValueError('S3 files must be opened in binary mode')

            if '+' in mode:
                raise ValueError('S3 files do not support "+" mode')

            mode = mode.replace('b', '')

            if mode not in {'r', 'w'}:
                raise ValueError(f'Invalid mode: {mode}')

            if mode == 'r':
                obj = self.client.get_object(Bucket=bucket_name, Key=path)
                stream = StreamingBody(raw_stream=obj['Body'], content_length=obj['ContentLength'])

                return S3ReadableFile(stream)
            elif mode == 'w':
                return S3WritableFile(bucket_name, path, self.client)
        except FileNotFoundError:
            if 'w' in mode or 'a' in mode:
                return S3WritableFile(bucket_name, path, self.client)
            else:
                raise

    async def exists(self, path: str | PurePath) -> bool:
        try:
            self._get_tree_item(self.tree, path)
        except FileNotFoundError:
            return False

        return True

    async def cp(self, src_path: str | PurePath, dst_path: str | PurePath):
        src_path = PurePath(src_path)
        dst_path_is_dir = isinstance(dst_path, str) and (dst_path.endswith('/') or dst_path.endswith('\\'))
        dst_path = PurePath(dst_path)
        src_item = self._get_tree_item(self.tree, src_path)
        src_is_dir = isinstance(src_item, collections.abc.Mapping)

        try:
            dst_item = self._get_tree_item(self.tree, dst_path)

            if isinstance(dst_item, collections.abc.Mapping):
                if src_is_dir:
                    pass
                else:
                    self._set_tree_item(self.tree, dst_path.joinpath(src_path.name), src_item)
            else:
                if dst_path_is_dir:
                    raise ValueError(f'Unable to copy {src_path} to directory path. it is a file')

                if src_is_dir:
                    raise ValueError(f'Unable to copy directory {src_path} to file {dst_path}')
                else:
                    self._set_tree_item(self.tree, dst_path, src_item)

                self._set_tree_item(self.tree, dst_path, src_item)
        except FileNotFoundError:
            if src_is_dir:
                pass
            else:
                if dst_path_is_dir:
                    self._set_tree_item(self.tree, dst_path.joinpath(src_path.name), src_item)
                else:
                    self._set_tree_item(self.tree, dst_path, src_item)

    async def mkdir(self, path: str | PurePath):
        return

    async def mkdirs(self, path: str | PurePath):
        return

    async def mv(self, src_path: str | PurePath, dst_path: str | PurePath):
        await self.cp(src_path, dst_path)
        await self.rm(src_path)

    async def rm(self, path: str | PurePath):
        """
        Removes file
        """

        if await self.is_dir(path):
            bucket_name, path = self._split_path(path)
            key_list = self.resource.Bucket(bucket_name).objects.filter(Prefix=f'{path}/')

            for key in key_list:
                self.resource.Object(bucket_name, key.key).delete()
        else:
            bucket_name, path = self._split_path(path)
            self.resource.Object(bucket_name, path).delete()

    async def is_dir(self, path: str | PurePath) -> bool:
        return isinstance(self._get_tree_item(self.tree, path), collections.abc.Mapping)

    async def glob(self, pattern: str) -> Tuple:
        pass


def _get_minio_client(endpoint_url: str, region_name: str, access_key_id: SecretStr, secret_access_key: SecretStr,
                      secure: bool = True) -> Minio:
    http_client = urllib3.PoolManager(
        timeout=urllib3.util.Timeout(connect=300, read=300),
        maxsize=10,
        cert_reqs='CERT_REQUIRED' if secure else 'CERT_NONE',
        retries=urllib3.Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        )
    )

    client = Minio(
        endpoint_url,
        region=region_name,
        access_key=access_key_id.get_secret_value(),
        secret_key=secret_access_key.get_secret_value(),
        http_client=http_client,
        secure=secure
    )

    return client


class MinioProtocol(BaseProtocol):
    def __init__(self, endpoint_url: str, region_name: str, access_key_id: SecretStr, secret_access_key: SecretStr,
                 secure: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client: Minio = _get_minio_client(endpoint_url, region_name, access_key_id, secret_access_key, secure)

    @staticmethod
    def _split_path(path: str | PurePath):
        bucket_name, *parts = super()._split_path(path)
        bucket_path = str(PurePath('/', *parts))

        return bucket_name, bucket_path

    def ls(self, path: str, pattern: str = None, *args, **kwargs) -> Sequence:
        recursive = bool(kwargs.get('recursive'))
        bucket_name, prefix = self._split_path(path)

        objects = self.client.list_objects(bucket_name, prefix, recursive)

        return tuple(objects)

    @asynccontextmanager
    def open(self, path: str, *args, **kwargs):
        raise NotImplemented

    def exists(self, path: str) -> bool:
        raise NotImplemented

    def cp(self, src_path: str, dst_path: str):
        raise NotImplemented

    def mkdir(self, path: str):
        raise NotImplemented

    def mkdirs(self, path: str):
        raise NotImplemented

    def mv(self, src_path: str, dst_path: str):
        raise NotImplemented

    def rm(self, path: str | PurePath):
        """
        Removes file
        """

        if self.is_dir(path):
            bucket_name, path = self._split_path(path)
            key_list = self.resource.Bucket(bucket_name).objects.filter(Prefix=f'{path}/')

            for key in key_list:
                self.resource.Object(bucket_name, key.key).delete()
        else:
            bucket_name, path = self._split_path(path)
            self.resource.Object(bucket_name, path).delete()

    def is_dir(self, path: str) -> bool:
        raise NotImplemented

    def glob(self, pattern: str) -> Tuple:
        raise NotImplemented

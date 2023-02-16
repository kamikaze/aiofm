import collections.abc
import logging
from contextlib import asynccontextmanager
from pathlib import PurePath
from typing import Sequence, Tuple

import aioboto3
import urllib3
from minio import Minio
from pydantic import SecretStr

from aiofm.helpers import ContextualBytesIO, ContextualStringIO
from aiofm.protocols import BaseProtocol


logger = logging.getLogger(__name__)


class S3Protocol(BaseProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__init_resource(*args, **kwargs)

    def __init_resource(self, *args, **kwargs):
        try:
            params = kwargs['params']
            credentials = params['auth']
            self.resource = aioboto3.resource(
                's3', aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['access_secret'],
                endpoint_url=params['S3_ENDPOINT_URL'], verify=params['VERIFY_SSL_CERTIFICATES']
            )
        except (TypeError, KeyError):
            logger.warning('No auth params has been provided for S3')

            self.resource = None

    @staticmethod
    def _split_path(path: str | PurePath) -> Sequence[str]:
        try:
            path_parts = path.parts
        except AttributeError:
            path_parts = PurePath(path).parts

        if path_parts[0] == '/':
            return path_parts[1], PurePath(*path_parts[2:])

        return path_parts[0], PurePath(*path_parts[1:])

    async def ls(self, path: str | PurePath, pattern: str = None, *args, **kwargs) -> Sequence:
        bucket_name, path = self._split_path(path)
        path = f'{path}/'

        objects = self.resource.Bucket(bucket_name).objects.filter(Prefix=path)

        keys = tuple(filter(
            None, (obj_summary.key.replace(path, '', 1).split('/', 1)[0] for obj_summary in objects)
        ))

        if keys:
            return keys

        raise FileNotFoundError

    @asynccontextmanager
    async def open(self, path: str | PurePath, *args, **kwargs):
        mode = kwargs.pop('mode', args[0] if len(args) else 'r')

        try:
            encoding = kwargs.get('encoding', 'utf-8')
            item = self._get_tree_item(self.tree, path)

            if 'b' in mode:
                f = ContextualBytesIO(item)
            else:
                f = ContextualStringIO(item.decode(encoding))

            yield f

            if 'w' in mode:
                if 'b' in mode:
                    item = f.getvalue()
                else:
                    item = f.getvalue().encode(encoding)

                self._set_tree_item(self.tree, path, item)
        except FileNotFoundError:
            if 'w' in mode or 'a' in mode:
                f = ContextualStringIO(None, *args, **kwargs)

                yield f

                self._set_tree_item(self.tree, path, f)
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

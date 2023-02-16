import collections.abc
import operator
from contextlib import asynccontextmanager
from functools import reduce
from pathlib import PurePath
from typing import Any, Mapping, Sequence, Tuple

from aiofm.helpers import ContextualBytesIO, ContextualStringIO
from aiofm.protocols import BaseProtocol


class MemoryProtocol(BaseProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tree = {}

    @staticmethod
    def _split_path(path: str | PurePath):
        try:
            path_parts = path.parts
        except AttributeError:
            path_parts = PurePath(path).parts

        return path_parts

    @classmethod
    def _remove_tree_item(cls, tree: Mapping, path: str | PurePath):
        path_parts = cls._split_path(path)

        try:
            del reduce(operator.getitem, path_parts[:-1], tree)[path_parts[-1]]
        except KeyError:
            pass

    @classmethod
    def _get_tree_item(cls, tree: Mapping, path: str | PurePath):
        path_parts = cls._split_path(path)

        try:
            return reduce(operator.getitem, path_parts, tree)
        except KeyError:
            raise FileNotFoundError

    @classmethod
    def _set_tree_item(cls, tree: Mapping, path: str | PurePath, value: Any):
        path_parts = cls._split_path(path)
        current_node = tree

        for path_part in path_parts[:-1]:
            try:
                current_node = current_node[path_part]

                if not isinstance(current_node, collections.abc.Mapping):
                    raise FileNotFoundError(f'Node already exists: {current_node}')
            except KeyError:
                new_node = {}
                current_node[path_part] = new_node
                current_node = new_node

        try:
            if type(current_node[path_parts[-1]]) != type(value):
                raise FileNotFoundError('Node already exists')

            current_node[path_parts[-1]] = value
        except KeyError:
            current_node[path_parts[-1]] = value

    @classmethod
    def _set_parent_tree_item(cls, tree: Mapping, path: str | PurePath, value: Any):
        try:
            parent_item_path = path.parent
        except AttributeError:
            parent_item_path = PurePath(path).parent

        cls._set_tree_item(tree, parent_item_path, value)

    async def ls(self, path: str | PurePath, pattern: str = None, *args, **kwargs) -> Sequence:
        item = self._get_tree_item(self.tree, path)

        return tuple(item)

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
        await self.mkdirs(path)

    async def mkdirs(self, path: str | PurePath):
        self._set_tree_item(self.tree, path, {})

    async def mv(self, src_path: str | PurePath, dst_path: str | PurePath):
        await self.cp(src_path, dst_path)
        await self.rm(src_path)

    async def rm(self, path: str | PurePath):
        self._remove_tree_item(self.tree, path)

    async def is_dir(self, path: str | PurePath) -> bool:
        return isinstance(self._get_tree_item(self.tree, path), collections.abc.Mapping)

    async def glob(self, pattern: str) -> Tuple:
        pass

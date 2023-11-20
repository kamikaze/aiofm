from typing import Dict, Generator

import pytest


@pytest.fixture(scope='session')
def fs_tree() -> Dict[str, Dict]:
    return {
        'tmp': {
            'existing_empty_dir': {},
            'existing_dir': {
                'another_existing.txt': b'another data'
            },
            'existing.txt': b'data data data',
        },
    }


@pytest.fixture(scope='session')
def fs_list(fs_tree) -> Dict:
    def flatten(root) -> Generator[tuple, None, None]:
        for k, v in root.items():
            if type(v) is dict:
                for sub_k, sub_v in flatten(v):
                    yield f'{k.rstrip("/")}/{sub_k}', sub_v
            else:
                yield k, v

    return dict(flatten(fs_tree))


@pytest.fixture(scope='function')
def s3_client(mocker, fs_list):
    page_size = 10
    bucket_objects = []
    page = []

    for idx, key in enumerate(fs_list):
        if idx % page_size == 0:
            page = []
            bucket_objects.append(page)

        obj = mocker.MagicMock()
        obj.key = key
        page.append(obj)

    async def async_filter(*args, **kwargs):
        return bucket_objects

    async def async_paginate(*args, **kwargs):
        for _page in bucket_objects:
            for item in _page:
                yield item

    client_mock = mocker.MagicMock()
    paginator_mock = mocker.MagicMock()
    paginator_mock.paginate.return_value = async_paginate
    client_mock.get_paginator.return_value = paginator_mock

    return client_mock

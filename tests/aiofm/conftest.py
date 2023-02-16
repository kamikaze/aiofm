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
def s3_resource(mocker, fs_list):
    bucket_objects = []

    for key in fs_list:
        obj = mocker.MagicMock()
        obj.key = key
        bucket_objects.append(obj)

    resource = mocker.MagicMock()
    bucket = mocker.MagicMock()
    bucket.objects.filter.return_value = bucket_objects
    resource.Bucket.return_value = bucket

    return resource

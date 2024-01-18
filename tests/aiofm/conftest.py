from typing import Dict, Generator
from unittest.mock import patch

import pytest
from aiobotocore.session import AioSession


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


@pytest.fixture
async def aioboto3_session():
    async with AioSession() as session:
        yield session


@pytest.fixture
async def s3_client(aioboto3_session):
    yield aioboto3_session.create_client('s3')


@pytest.fixture
def s3_protocol_mock(aioboto3_session, s3_client):
    with patch('aiofm.protocols.s3.get_session', return_value=aioboto3_session), \
         patch('aiofm.protocols.s3.BaseProtocol.get_session', return_value=aioboto3_session):

        # Set up the S3 client mock
        page_mock = {'Contents': [{'Key': 'file1.txt'}, {'Key': 'file2.txt'}]}
        s3_client.paginate.return_value.__aiter__.return_value.__anext__.side_effect = [
            {'Contents': []},  # Empty page to exit the loop
            page_mock
        ]

        yield s3_client

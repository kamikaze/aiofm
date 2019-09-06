import pytest

from aiofm.protocols import MemProtocol


@pytest.mark.asyncio
async def test_ls_tmp_dir():
    fs = MemProtocol()
    fs._tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.ls('/tmp') == ('xxx', 'a.txt')


@pytest.mark.asyncio
async def test_ls_inextisting_dir_fails():
    fs = MemProtocol()
    fs._tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        assert await fs.ls('/pmt')

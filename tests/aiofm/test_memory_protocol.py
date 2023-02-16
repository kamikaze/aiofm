import pytest

from aiofm.protocols.memory import MemoryProtocol


@pytest.mark.asyncio
async def test_ls_tmp_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.ls('/tmp') == ('xxx', 'a.txt')


@pytest.mark.asyncio
async def test_ls_inextisting_dir_fails():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        assert await fs.ls('/pmt')


@pytest.mark.asyncio
async def test_existing_file_exists():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/tmp/a.txt') is True


@pytest.mark.asyncio
async def test_inexisting_file_does_not_exist():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/tmp/b.txt') is False


@pytest.mark.asyncio
async def test_existing_dir_exists():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/tmp/xxx') is True


@pytest.mark.asyncio
async def test_inexisting_dir_does_not_exist():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/tmp/yyy') is False


@pytest.mark.asyncio
async def test_existing_path_exists():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/tmp/a.txt') is True


@pytest.mark.asyncio
async def test_inexisting_path_does_not_exist():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.exists('/pmt/a.txt') is False


@pytest.mark.asyncio
async def test_file_is_not_a_directory():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.is_dir('/tmp/a.txt') is False


@pytest.mark.asyncio
async def test_directory_is_a_direcotry():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    assert await fs.is_dir('/tmp/xxx') is True


@pytest.mark.asyncio
async def test_inexisting_path_is_dir_check_fails():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        assert await fs.is_dir('/tmp/yyy.txt')


@pytest.mark.asyncio
async def test_mkdir_first_toplevel():
    fs = MemoryProtocol()

    await fs.mkdir('/home')

    assert fs.tree == {'/': {'home': {}}}


@pytest.mark.asyncio
async def test_mkdir_nested():
    fs = MemoryProtocol()
    fs.tree = {'/': {'home': {}}}

    await fs.mkdir('/home/user')

    assert fs.tree == {'/': {'home': {'user': {}}}}


@pytest.mark.asyncio
async def test_mkdir_existing_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'home': {'user': {}}}}

    await fs.mkdir('/home/user')

    assert fs.tree == {'/': {'home': {'user': {}}}}


@pytest.mark.asyncio
async def test_mkdir_nested_within_inexisting_path():
    fs = MemoryProtocol()
    fs.tree = {'/': {}}

    await fs.mkdir('/home/user/documents')

    assert fs.tree == {'/': {'home': {'user': {'documents': {}}}}}


@pytest.mark.asyncio
async def test_mkdir_first_nested():
    fs = MemoryProtocol()

    await fs.mkdir('/home/user/documents')

    assert fs.tree == {'/': {'home': {'user': {'documents': {}}}}}


@pytest.mark.asyncio
async def test_mkdir_fails_when_path_is_a_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        await fs.mkdir('/tmp/a.txt')

    with pytest.raises(FileNotFoundError):
        await fs.mkdir('/tmp/a.txt/dir')


@pytest.mark.asyncio
async def test_mkdirs_first_toplevel():
    fs = MemoryProtocol()

    await fs.mkdirs('/home')

    assert fs.tree == {'/': {'home': {}}}


@pytest.mark.asyncio
async def test_mkdirs_nested():
    fs = MemoryProtocol()
    fs.tree = {'/': {'home': {}}}

    await fs.mkdirs('/home/user')

    assert fs.tree == {'/': {'home': {'user': {}}}}


@pytest.mark.asyncio
async def test_mkdirs_existing_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'home': {'user': {}}}}

    await fs.mkdirs('/home/user')

    assert fs.tree == {'/': {'home': {'user': {}}}}


@pytest.mark.asyncio
async def test_mkdirs_nested_within_inexisting_path():
    fs = MemoryProtocol()
    fs.tree = {'/': {}}

    await fs.mkdirs('/home/user/documents')

    assert fs.tree == {'/': {'home': {'user': {'documents': {}}}}}


@pytest.mark.asyncio
async def test_mkdirs_first_nested():
    fs = MemoryProtocol()

    await fs.mkdirs('/home/user/documents')

    assert fs.tree == {'/': {'home': {'user': {'documents': {}}}}}


@pytest.mark.asyncio
async def test_mkdirs_fails_when_path_is_a_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        await fs.mkdirs('/tmp/a.txt')

    with pytest.raises(FileNotFoundError):
        await fs.mkdirs('/tmp/a.txt/dir')


@pytest.mark.asyncio
async def test_rm_empty_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'home': {'user': {'documents': {}}}}}

    await fs.rm('/home/user/documents')

    assert fs.tree == {'/': {'home': {'user': {}}}}


@pytest.mark.asyncio
async def test_rm_non_empty_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.rm('/tmp')

    assert fs.tree == {'/': {}}


@pytest.mark.asyncio
async def test_rm_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.rm('/tmp/a.txt')

    assert fs.tree == {'/': {'tmp': {'xxx': {}}}}


@pytest.mark.asyncio
async def test_rm_inexisting_dir():
    fs = MemoryProtocol()

    await fs.rm('/home/user/documents')

    assert fs.tree == {}

    fs.tree = {'/': {'home': {'user': {'documents': {}}}}}

    await fs.rm('/tmp')

    assert fs.tree == {'/': {'home': {'user': {'documents': {}}}}}


@pytest.mark.asyncio
async def test_mv_file_to_existing_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.mv('/tmp/a.txt', '/tmp/xxx')

    assert fs.tree == {'/': {'tmp': {'xxx': {'a.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_mv_file_to_inexisting_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.mv('/tmp/a.txt', '/tmp/yyy/')

    assert fs.tree == {'/': {'tmp': {'xxx': {}, 'yyy': {'a.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_mv_file_to_existing_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {'b.txt': b''}, 'a.txt': b'data data data'}}}

    await fs.mv('/tmp/a.txt', '/tmp/xxx/b.txt')

    assert fs.tree == {'/': {'tmp': {'xxx': {'b.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_mv_file_to_inexisting_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.mv('/tmp/a.txt', '/tmp/xxx/c.txt')

    assert fs.tree == {'/': {'tmp': {'xxx': {'c.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_mv_dir_to_existing_file_should_fail():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(ValueError, match='Unable to copy directory /tmp/xxx to file /tmp/a.txt'):
        await fs.mv('/tmp/xxx', '/tmp/a.txt')


@pytest.mark.asyncio
async def test_cp_file_to_existing_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.cp('/tmp/a.txt', '/tmp/xxx')

    assert fs.tree == {'/': {'tmp': {'a.txt': b'data data data', 'xxx': {'a.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_cp_file_to_inexisting_dir():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.cp('/tmp/a.txt', '/tmp/yyy/')

    assert fs.tree == {'/': {'tmp': {'a.txt': b'data data data', 'xxx': {}, 'yyy': {'a.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_cp_file_to_existing_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {'b.txt': b''}, 'a.txt': b'data data data'}}}

    await fs.cp('/tmp/a.txt', '/tmp/xxx/b.txt')

    assert fs.tree == {'/': {'tmp': {'a.txt': b'data data data', 'xxx': {'b.txt': b'data data data'}}}}


@pytest.mark.asyncio
async def test_cp_file_to_inexisting_file():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    await fs.cp('/tmp/a.txt', '/tmp/xxx/c.txt')

    assert fs.tree == {'/': {'tmp': {'xxx': {'c.txt': b'data data data'}, 'a.txt': b'data data data'}}}


@pytest.mark.asyncio
async def test_cp_dir_to_existing_file_should_fail():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(ValueError, match='Unable to copy directory /tmp/xxx to file /tmp/a.txt'):
        await fs.cp('/tmp/xxx', '/tmp/a.txt')


@pytest.mark.asyncio
async def test_open_inexisting_file_for_read_should_fail():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    with pytest.raises(FileNotFoundError):
        async with fs.open('/tmp/b.txt'):
            pass


@pytest.mark.asyncio
async def test_open_existing_file_for_read():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    async with fs.open('/tmp/a.txt') as f:
        assert f.read() == 'data data data'


@pytest.mark.asyncio
async def test_unclosed_file_does_not_change_fs():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    async with fs.open('/tmp/a.txt', mode='w') as f:
        f.write('TEST TEST TEST')

        assert fs.tree['/']['tmp']['a.txt'] == b'data data data'


@pytest.mark.asyncio
async def test_closed_file_changes_fs():
    fs = MemoryProtocol()
    fs.tree = {'/': {'tmp': {'xxx': {}, 'a.txt': b'data data data'}}}

    async with fs.open('/tmp/a.txt', mode='w') as f:
        f.write('TEST TEST TEST')

    assert fs.tree['/']['tmp']['a.txt'] == b'TEST TEST TEST'

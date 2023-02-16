from collections import defaultdict
from io import StringIO, BytesIO


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


class ContextualStringIO(StringIO):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close()
        return False


class ContextualBytesIO(BytesIO):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close()
        return False

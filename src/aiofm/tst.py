from pathlib import PurePath

from aiofm.protocols.s3 import S3Protocol


def main():
    protocol = S3Protocol()

    for item in protocol.ls('rtu-datasets/own_transport/'):
        path = item['Key']

        if path == 'own_transport/':
            continue

        print(path)

        with protocol.open(f'rtu-datasets/{path}', 'rb') as fi, open(PurePath(path).name, 'wb') as fo:
            for chunk in fi:
                fo.write(chunk)


if __name__ == '__main__':
    main()

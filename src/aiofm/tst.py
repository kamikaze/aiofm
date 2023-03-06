from aiofm.protocols.s3 import S3Protocol


def main():
    protocol = S3Protocol()

    for path in protocol.ls('/rtu-datasets/own_transport/'):
        if str(path) == '/rtu-datasets/own_transport':
            continue

        print(path)

        with protocol.open(path, 'rb') as fi, protocol.open(f'{path}.copy', 'wb') as fo:
            for chunk in fi:
                fo.write(chunk)

        with protocol.open(path, 'rb') as fi, protocol.open(f'{path}.copy', 'wb') as fo:
            fo.write(fi)


if __name__ == '__main__':
    main()

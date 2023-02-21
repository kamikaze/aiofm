from aiofm.protocols.s3 import S3Protocol


def main():
    protocol = S3Protocol()
    items = protocol.ls('rtu-datasets/own_transport/')
    print(items)


if __name__ == '__main__':
    main()

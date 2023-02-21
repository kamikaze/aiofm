from aiofm.protocols.s3 import S3Protocol


def main():
    protocol = S3Protocol()

    for item in protocol.ls('rtu-datasets/own_transport/'):
        print(item['Key'])


if __name__ == '__main__':
    main()

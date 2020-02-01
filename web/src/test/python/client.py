from __future__ import print_function

import grpc

import bmservice_pb2
import bmservice_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:9999')
    stub = bmservice_pb2_grpc.PostBitmapStub(channel)
    response = stub.getBitmap(bmservice_pb2.BitmapRequest(bizname='testme',date='20190219'))
    print("Greeter client received: " + str(response.highbits))
    print("Greeter client received: " + str(len(response.bitmaps)))
    channel.close()
if __name__ == '__main__':
    run()

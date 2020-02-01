# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import bmservice_pb2 as bmservice__pb2


class PostBitmapStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.getBitmap = channel.unary_unary(
        '/RPCBitmap.PostBitmap/getBitmap',
        request_serializer=bmservice__pb2.BitmapRequest.SerializeToString,
        response_deserializer=bmservice__pb2.BitmapReply.FromString,
        )


class PostBitmapServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def getBitmap(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_PostBitmapServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'getBitmap': grpc.unary_unary_rpc_method_handler(
          servicer.getBitmap,
          request_deserializer=bmservice__pb2.BitmapRequest.FromString,
          response_serializer=bmservice__pb2.BitmapReply.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'RPCBitmap.PostBitmap', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))

# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import nfp_pb2 as nfp__pb2


class NfpStub(object):
  """The greeting service definition.
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.create_network_function = channel.unary_unary(
        '/helloworld.Nfp/create_network_function',
        request_serializer=nfp__pb2.NfpRequest.SerializeToString,
        response_deserializer=nfp__pb2.NfpResponse.FromString,
        )


class NfpServicer(object):
  """The greeting service definition.
  """

  def create_network_function(self, request, context):
    """Sends a greeting
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_NfpServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'create_network_function': grpc.unary_unary_rpc_method_handler(
          servicer.create_network_function,
          request_deserializer=nfp__pb2.NfpRequest.FromString,
          response_serializer=nfp__pb2.NfpResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'helloworld.Nfp', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))

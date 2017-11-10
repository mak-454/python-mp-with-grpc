
from __future__ import print_function

import grpc

from gbpservice.nfp.ahmed import nfp_pb2
from gbpservice.nfp.ahmed import nfp_pb2_grpc


def run():
  channel = grpc.insecure_channel('localhost:50054')
  stub = nfp_pb2_grpc.NfpStub(channel)
  nfpdata = "{'name': 'ahmed'}"
  response = stub.create_network_function(nfp_pb2.NfpRequest(namespace='orchestrator',data=nfpdata))
  print("received: " + response.message)


if __name__ == '__main__':
  run()


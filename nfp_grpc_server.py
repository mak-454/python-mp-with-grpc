from concurrent import futures
import time

import grpc

from gbpservice.nfp.ahmed import nfp_pb2
from gbpservice.nfp.ahmed import nfp_pb2_grpc

class NfpGrpcServer(nfp_pb2_grpc.NfpServicer):

  def create_network_function(self, request, context):
    print "HELOOOOOOOOO"
    import pdb;pdb.set_trace()

def launch_grpc_server():
    import pdb;pdb.set_trace()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    nfp_pb2_grpc.add_NfpServicer_to_server(NfpGrpcServer(), server)
    server.add_insecure_port('[::]:50054')
    server.start()
    return server

if __name__ == '__main__':
  server = launch_grpc_server()
  _ONE_DAY_IN_SECONDS = 60 * 60 * 24
  try:
    while True:
      time.sleep(_ONE_DAY_IN_SECONDS)
  except KeyboardInterrupt:
    server.stop(0)


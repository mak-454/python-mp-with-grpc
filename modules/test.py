import time

from oslo_log import helpers as log_helpers
import oslo_messaging

from gbpservice.nfp.common import topics as nfp_rpc_topics



from gbpservice.nfp.ahmed.core.event import Event
from gbpservice.nfp.ahmed.core import module as nfp_api
from gbpservice.nfp.ahmed.core import path as nfp_path
from gbpservice.nfp.ahmed.core.rpc import RpcAgent


class RpcHandler(object):

    """RPC Handler for Node Driver to NFP.

    Network Function methods are invoked in an RPC Call by the
    node driver and data has to be returned by the orchestrator.
    """

    RPC_API_VERSION = '1.0'
    target = oslo_messaging.Target(version=RPC_API_VERSION)

    def __init__(self, conf, controller):
        super(RpcHandler, self).__init__()
        self.conf = conf
        self._controller = controller
        # REVISIT (mak): Can a ServiceOrchestrator object be
        # initialized here and used for each rpc ?

    @log_helpers.log_method_call
    def create_network_function(self, context, network_function):
        print "hello"
        ev = self._controller.new_event(id='CREATE_NETWORK_FUNCTION')
        self._controller.post_event(ev)

def rpc_init(controller, config):
    rpcmgr = RpcHandler(config, controller)
    agent = RpcAgent(controller,
                     host=config.host,
                     topic='orchestrator',
                     manager=rpcmgr)
    controller.register_rpc_agents([agent])


class EventHandler(nfp_api.NfpEventHandler):
    def __init__(self, controller):
	self._controller = controller

    def handle_event(self, event):
       	print "HURRRAAAAYYYYYY"
	if event.id == 'CREATE_NETWORK_FUNCTION':
		ev = self._controller.new_event(id='CREATE_NETWORK_FUNCTION1')
		self._controller.post_event(ev)

def events_init(controller, config, event_handler):
    events = ['CREATE_NETWORK_FUNCTION', 'CREATE_NETWORK_FUNCTION1']
    events_to_register = []
    for event in events:
        events_to_register.append(
            Event(id=event, handler=event_handler))
    controller.register_events(events_to_register)

def nfp_module_init(controller, config):
    rpc_init(controller, config)
    events_init(controller, config, EventHandler(controller))


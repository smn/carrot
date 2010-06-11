from carrot.backends import base
from os.path import join, abspath, dirname
from .amqp import AmqpFactory, AmqpProtocol

DEFAULT_PORT = 5672
DEFAULT_SPEC = join(abspath(dirname(__file__)), 'amqp-spec-0-8.xml')

class Backend(base.BaseBackend):
    """txamqp backend"""
    
    default_port = DEFAULT_PORT
    default_spec = DEFAULT_SPEC
    
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.default_port = kwargs.get("default_port", self.default_port)
        self.default_spec = kwargs.get('default_spec', self.default_spec)
        self._channel_ref = None
    
    @property
    def channel(self):
        """If no channel exists, a new one is requested."""
        if not self._channel:
            self._channel_ref = weakref.ref(self.connection.get_channel())
        return self._channel
    
    def ack(self, delivery_tag):
        """docstring for ack"""
        pass
    
    def cancel(self, *args, **kwargs):
        """docstring for cancel"""
        pass
    
    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.connection
        if not conninfo.hostname:
            raise KeyError("Missing hostname for AMQP connection.")
        if conninfo.userid is None:
            raise KeyError("Missing user id for AMQP connection.")
        if conninfo.password is None:
            raise KeyError("Missing password for AMQP connection.")
        if not conninfo.port:
            conninfo.port = self.default_port
        return AmqpFactory(host=conninfo.hostname, 
                            port=conninfo.port,
                            vhost=conninfo.virtual_host, 
                            user=conninfo.userid, 
                            password=conninfo.password, 
                            spec_file=self.default_spec)

    
    
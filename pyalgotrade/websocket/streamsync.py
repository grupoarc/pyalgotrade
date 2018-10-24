
from .. import logger as pyalgo_logger

logger = pyalgo_logger.getLogger(__name__)

def noop(*args, **kwargs): pass

log = logger.debug

class StreamSynchronizer(object):
    """
    An abstraction of the common pattern for synchronizing streaming data:
      1.  connect to streaming source, queue incoming data
      2.  connect to static source, fetch data with some syncpoint
      3.  process queue, ignore data with syncpoint older than that of (2)
      4.  process incoming data
      nb. a syncpoint can be anything (timestamp, sequence number, etc)
    """

    def __init__(self,
            syncpoint_from_streamdata,
            streamdata_newer_than_syncpoint,
            streamdata_processor,
            syncdata_process_and_return_syncpoint):
        """
        Constructor takes four functions:
        syncpoint_from_streamdata - streamdata -> syncpoint
        streamdata_newer_than_syncpoint - (syncpoint, streamdata) -> Bool
        streamdata_processor - (streamdata) -> None (side effects)
        syncdata_process_and_return_syncpoint - (syncdata) -> syncpoint (side effects)
        """

        self.syncpoint_from_streamdata = syncpoint_from_streamdata
        self.streamdata_newer_than_syncpoint = streamdata_newer_than_syncpoint
        self.streamdata_processor = streamdata_processor
        self.syncdata_process_and_return_syncpoint = syncdata_process_and_return_syncpoint

        self.streamdata_handler = self.submit_streamdata_presync
        self.syncpoint = None
        self.queue = []
        log("initialized a streamsyncer")

    def submit_streamdata_presync(self, data):
        self.queue.append(data)
        if self.syncpoint is None:
            log("queued incoming message #%d" % len(self.queue))
            return
        log("Queue is %d long" % len(self.queue))
        while self.queue:
            m = self.queue.pop(0)
            if not self.streamdata_newer_than_syncpoint(self.syncpoint, m):
                log("Dropping pre-sync message")
                continue
            self.streamdata_handler = self.streamdata_processor
            self.streamdata_processor(m)
            log("switched handler")

    def submit_streamdata(self, data):
        #log("got streamdata")
        self.streamdata_handler(data)

    def submit_syncdata(self, data):
        self.syncpoint = self.syncdata_process_and_return_syncpoint(data)
        log("Set syncpoint to %r" % self.syncpoint)



# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import json
import time

from ws4py.client import tornadoclient
import tornado
import pyalgotrade.logger

logger = pyalgotrade.logger.getLogger("websocket.client")


# This class is responsible for sending keep alive messages and detecting disconnections
# from the server.
class KeepAliveMgr(object):
    def __init__(self, wsClient, maxInactivity, responseTimeout):
        assert(maxInactivity > 0)
        assert(responseTimeout > 0)
        self.__callback = None
        self.__wsClient = wsClient
        self.__activityTimeout = maxInactivity
        self.__responseTimeout = responseTimeout
        self.__lastSeen = None
        self.__kaSent = None  # timestamp when the last keep alive was sent.

    def _keepAlive(self):
        if self.__lastSeen is None:
            return

        # Check if we're under the inactivity threshold.
        inactivity = (time.time() - self.__lastSeen)
        if inactivity <= self.__activityTimeout:
            return

        # Send keep alive if it was not sent,
        # or check if we have to timeout waiting for the keep alive response.
        try:
            if self.__kaSent is None:
                self.sendKeepAlive()
                self.__kaSent = time.time()
            elif (time.time() - self.__kaSent) > self.__responseTimeout:
                self.__wsClient.onDisconnectionDetected()
        except Exception as e:
            # Treat an error sending the keep-alive as a diconnection.
            print("Error sending keep alive", e)
            self.__wsClient.onDisconnectionDetected()

    def getWSClient(self):
        return self.__wsClient

    def setAlive(self):
        self.__lastSeen = time.time()
        self.__kaSent = None

    def start(self):
        # Check every second.
        self.__callback = tornado.ioloop.PeriodicCallback(self._keepAlive, 1000, self.__wsClient.getIOLoop())
        self.__callback.start()

    def stop(self):
        if self.__callback is not None:
            self.__callback.stop()

    # Override to send the keep alive msg.
    def sendKeepAlive(self):
        raise NotImplementedError()

    # Return True if the response belongs to a keep alive message, False otherwise.
    def handleResponse(self, msg):
        raise NotImplementedError()


# Base clase for websocket clients.
# To use it call connect and startClient, and stopClient.
class WebSocketClientBase(tornadoclient.TornadoWebSocketClient):
    def __init__(self, url, *a, **kw):
        super(WebSocketClientBase, self).__init__(url, *a, **kw)
        self.__keepAliveMgr = None
        self.__connected = False
        self.__ioloop = tornado.ioloop.IOLoop.current()
        logger.debug("initing websocketclientbase")

    # This is to avoid a stack trace because TornadoWebSocketClient is not implementing _cleanup.
    def _cleanup(self):
        logger.debug("cleaning up")
        ret = None
        try:
            ret = super(WebSocketClientBase, self)._cleanup()
        except Exception:
            pass
        return ret

    def getIOLoop(self):
        return self.__ioloop

    # Must be set before calling startClient().
    def setKeepAliveMgr(self, keepAliveMgr):
        logger.debug("setting KeepAliveMgr to : "+ repr(keepAliveMgr))
        if self.__keepAliveMgr is not None:
            raise Exception("KeepAliveMgr already set")
        self.__keepAliveMgr = keepAliveMgr

    def received_message(self, message):
        #logger.debug("got message: " + str(message))
        try:
            msg = json.loads(message.data)

            if self.__keepAliveMgr is not None:
                self.__keepAliveMgr.setAlive()
                if self.__keepAliveMgr.handleResponse(msg):
                    return

            self.onMessage(msg)
        except Exception as e:
            self.onUnhandledException(e)

    def opened(self):
        logger.debug("opened")
        self.__connected = True
        if self.__keepAliveMgr is not None:
            self.__keepAliveMgr.start()
            self.__keepAliveMgr.setAlive()
        self.onOpened()

    def closed(self, code, reason=None):
        logger.debug("closed")
        wasConnected = self.__connected
        self.__connected = False
        if self.__keepAliveMgr:
            self.__keepAliveMgr.stop()
            self.__keepAliveMgr = None
        self.__ioloop.stop()

        if wasConnected:
            self.onClosed(code, reason)

    def isConnected(self):
        return self.__connected

    def startClient(self):
        logger.debug("Starting IOLoop")
        self.__ioloop.start()
        logger.debug("IOLoop started")

    def stopClient(self):
        logger.debug("Stopping Client")
        try:
            if self.__connected:
                self.close()
            self.close_connection()
        except Exception as e:
            logger.warning("Failed to close connection: %s" % (e))

    ######################################################################
    # Overrides

    def onUnhandledException(self, exception):
        logger.critical("Unhandled exception", exc_info=exception)
        self.stopClient()
        raise

    def onOpened(self):
        pass

    def onMessage(self, msg):
        raise NotImplementedError()

    def onClosed(self, code, reason):
        pass

    def onDisconnectionDetected(self):
        pass

#require "SPIFlashLogger.class.nut:2.1.0"
#require "ConnectionManager.class.nut:1.0.1"
#require "Serializer.class.nut:1.0.0"
#require "bullwinkle.class.nut:2.3.1"

const IMP_PAGER_MESSAGE_TIMEOUT = 1;
const IMP_PAGER_RETRY_PERIOD_SEC = 2;
const IMP_PAGER_ITERATE_OVER_RETRIES_PERIOD_SEC = 0.2;

const IMP_PAGER_CM_DEFAULT_SEND_TIMEOUT = 1;
const IMP_PAGER_CM_DEFAULT_BUFFER_SIZE = 8096;

class ImpPager {

    // Bullwinkle instance
    _bullwinkle = null;

    // ConnectionManager instance
    _connectionManager = null;

    // SPIFlashLogger instance
    _spiFlashLogger = null;

    // Message retry timer
    _retryTimer = null;

    // Map of message -> SPI Flash address
    _messageAddrMap = null;

    // Message counter used for generating unique message id
    _messageCounter = 0;

    constructor(connectionManager, bullwinkle = null, spiFlashLogger = null, debug = false) {
        _connectionManager = connectionManager;

        _bullwinkle = bullwinkle ? bullwinkle : Bullwinkle({"messageTimeout" : IMP_PAGER_MESSAGE_TIMEOUT});
        _spiFlashLogger = spiFlashLogger ? spiFlashLogger : SPIFlashLogger();

        _messageAddrMap = {}
        _messageCounter = 0;

        // Set ConnectionManager listeners
        _connectionManager.onConnect(_onConnect.bindenv(this));
        _connectionManager.onDisconnect(_onDisconnect.bindenv(this));

        // Schedule routine to retry sending messages
        _scheduleRetryIfConnected();
    }

    function send(messageName, data = null, onReply = null) {
        _send(messageName, data, onReply);
    }

    function _onSuccess(message) {
        // Erase message from the logger if it was cached
        if (message.id in _messageAddrMap) {
            local addr = _messageAddrMap[message.id];
            _log_debug("Erasing address: " + addr)
            _spiFlashLogger.erase(addr);
            _messageAddrMap.rawdelete(message);
        }
    }

    function _onFail(err, message, retry) {
        // On fail write the message to the SPI Flash for further processing
        if (!(message.id in _messageAddrMap)) {
            _messageAddrMap[message.id] <- null;
            _spiFlashLogger.write(message);
        }
    }

    function _send(messageName, data, onReply = null) {
        return _bullwinkle.send(messageName, data)
            .onSuccess(_onSuccess.bindenv(this))
            .onFail(_onFail.bindenv(this))
            .onReply(onReply);
    }

    /**
     * Generates unique message id. It should incrementally increase
     */
    function _getMessageUniqueId() {
        // Using a local counter seems good enough as there is almost 
        // no chance for subsiquent values to collide even if device reboots
        return date().time + "-" + (_messageCounter++);
    }

    function _retry() {
        _log_debug("Start processing pending messages...");
        _spiFlashLogger.read(
            function(dataPoint, addr, next) {
                // There's no point of retrying to send pending messages when disconnected
                if (!_connectionManager.isConnected()) {
                    // Abort scanning
                    next(false);
                    return;
                }
                _send(dataPoint.name, dataPoint.data);
                _messageAddrMap[dataPoint.id] <- addr;
                _log_debug("Reading from the SPI Flash: " + dataPoint.data.raw + " at addr: " + addr);
                imp.wakeup(IMP_PAGER_ITERATE_OVER_RETRIES_PERIOD_SEC, next);
            }.bindenv(this),
            function() {
                _log_debug("Finished processing all pending messages");
                _scheduleRetryIfConnected();
            }.bindenv(this)
        );
    }

    function _onConnect() {
        _log_debug("onConnect: scheduling pending message processor...");
        _scheduleRetryIfConnected();
    }

    function _onDisconnect(expected) {
        _log_debug("onDisconnect: cancelling pending message processor...");
        // Stop any attempts to process pending messages while we are disconnected
        _cancelRetryTimer();
    }

    function _cancelRetryTimer() {
        if (!_retryTimer) {
            return;
        }
        imp.cancelwakeup(_retryTimer);
        _retryTimer = null;
    }

    function _scheduleRetryIfConnected() {
        if (!_connectionManager.isConnected()) {
            return;
        }

        _cancelRetryTimer();
        _retryTimer = imp.wakeup(IMP_PAGER_RETRY_PERIOD_SEC, _retry.bindenv(this));
    }

    function _log_debug(str) {
        _connectionManager.log(str);
    }
}

class ImpPager.ConnectionManager extends ConnectionManager {

    // Global list of handlers to be called when device gets connected
    _onConnectHandlers = array();

    // Global list of handlers to be called when device gets disconnected
    _onDisconnectHandlers = array();

    constructor(settings = {}) {
        base.constructor(settings);

        // Override the timeout to make it a nonzero, but still 
        // a small value. This is needed to avoid accedental 
        // imp disconnects when using ConnectionManager library
        local sendTimeout = "sendTimeout" in settings ? 
            settings.sendTimeout : IMP_PAGER_CM_DEFAULT_SEND_TIMEOUT;
        server.setsendtimeoutpolicy(RETURN_ON_ERROR, WAIT_TIL_SENT, sendTimeout);

        // Set the recommended buffer size
        local sendBufferSize = "sendBufferSize" in settings ? 
            settings.sendBufferSize : IMP_PAGER_CM_DEFAULT_BUFFER_SIZE;
        imp.setsendbuffersize(sendBufferSize);

        // Seting onConnect/onDisconnect handlers
        base.onConnect(_onConnect);
        base.onDisconnect(_onDisconnect);
    }

    function _onConnect() {
        foreach (index, callback in _onConnectHandlers) {
            if (callback != null) {
                imp.wakeup(0, callback);
            }
        }
    }

    function _onDisconnect(expected) {
        foreach (index, callback in _onDisconnectHandlers) {
            if (callback != null) {
                imp.wakeup(0, function() {
                    callback(expected);
                });
            }
        }
    }

    function _addHandlerAndCleanupEmptyOnes(handlers, callback) {
        if (handlers.find(callback) == null) {
            handlers.append(callback.weakref());
        }
        return handlers.filter(
            function(index, value) {
                return value != null;
            }
        );
    }

    function onConnect(callback) {
        _onConnectHandlers = _addHandlerAndCleanupEmptyOnes(_onConnectHandlers, callback);
    }

    function onDisconnect(callback) {
        _onDisconnectHandlers = _addHandlerAndCleanupEmptyOnes(_onDisconnectHandlers, callback);
    }
};

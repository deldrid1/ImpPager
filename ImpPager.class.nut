#require "SPIFlashLogger.class.nut:2.1.0"
#require "ConnectionManager.class.nut:1.0.1"
#require "Serializer.class.nut:1.0.0"
#require "bullwinkle.class.nut:2.3.2"

const IMP_PAGER_MESSAGE_TIMEOUT = 1;
const IMP_PAGER_RETRY_PERIOD_SEC = 0.0;

const IMP_PAGER_CM_DEFAULT_SEND_TIMEOUT = 1;
const IMP_PAGER_CM_DEFAULT_BUFFER_SIZE = 8096;

const IMP_PAGER_RTC_INVALID_TIME = 946684800; //Saturday 1st January 2000 12:00:00 AM UTC - this is what time() returns if the RTC signal from the imp cloud has not been received this boot.

class ImpPager {

    // Bullwinkle instance
    _bullwinkle = null;

    // ConnectionManager instance
    _connectionManager = null;

    // SPIFlashLogger instance
    _spiFlashLogger = null;

    // Reference to the SPIFlashLogger onData next() callback function.
    _next = null;

    // Message retry timer
    _retryTimer = null;

    // The number of boots that this device has had - used to detect if multiple reboots without a RTC have occurred.  If bootNumber is not set, we will not attempt to recover from a lack of RTC.
    _bootNumber = null;

    // array of [lastTSmillis, lastTStimeSec] used for rebuilding timestamps for if/when we didn't have a RTC.
    _lastTS = null;

    // Debug flag that controlls the debug output
    _debug = false;

    constructor(connectionManager, bullwinkle = null, spiFlashLogger = null, bootNumber = null, debug = false) {
        _connectionManager = connectionManager;

        _bullwinkle = bullwinkle ? bullwinkle : Bullwinkle({"messageTimeout" : IMP_PAGER_MESSAGE_TIMEOUT});
        _spiFlashLogger = spiFlashLogger ? spiFlashLogger : SPIFlashLogger();

        // Set ConnectionManager listeners
        _connectionManager.onConnect(_onConnect.bindenv(this));
        _connectionManager.onDisconnect(_onDisconnect.bindenv(this));

        // Set the bootNumber.  If a BootNumber is provided, we will try to rebuild timestamps for conditions where we boot offline and without a RTC.  Otherwise, we won't.
        _bootNumber = bootNumber

        // Schedule routine to retry sending messages
        _scheduleRetryIfConnected();

        _debug = debug;
    }

    function send(messageName, data = null, ts = null) {
        if(ts == null) ts = time()
        if(_bootNumber != null && ts == IMP_PAGER_RTC_INVALID_TIME) ts = _bootNumber + "-" + hardware.millis()  //provides ms accurate delta times that can be up to 25 days (2^31ms) apart.  We use typeof(ts) == "string" to detect that our RTC has not been set in the .onFail.

        _bullwinkle.send(messageName, data, ts)
                    .onSuccess(_onSuccess.bindenv(this))
                    .onFail(_onFail.bindenv(this));
    }

    function _onSuccess(message) {
        // Do nothing
        _log_debug("ACKed message id " + message.id + " with name: '" + message.name + "' and data: " + message.data);
        if ("metadata" in message && "addr" in message.metadata && message.metadata.addr) {
            local addr = message.metadata.addr;
            _spiFlashLogger.erase(addr);
            _scheduleRetryIfConnected();
        }
    }

    function _onFail(err, message, retry) {
        _log_debug("Failed to deliver message id " + message.id + " with name: '" + message.name + "' and data: " + message.data + ", err: " + err);

        // On fail write the message to the SPI Flash for further processing
        // only if it's not already there.
        if (!("metadata" in message) || !("addr" in message.metadata) || !(message.metadata.addr)) {
            delete message.type //Not needed to write to SPIFlash, as the type will always be BULLWINKLE_MESSAGE_TYPE.SEND

            if(typeof(message.ts) == "string") {  // We have a _bootNumber and invalid RTC - add some metadata so that we can try to restore the timestamp once we have RTC.
                message.metadata <- {
                  "boot": split(message.ts, "-")[0].tointeger()
                  "rtc": false
                }
                message.ts = split(message.ts, "-")[1].tointeger()
            }
            _spiFlashLogger.write(message);
            message.type <- BULLWINKLE_MESSAGE_TYPE.TIMEOUT // We are mucking around with the internal logic of Bullwinkle so we need to repair the message object here
        }
        _scheduleRetryIfConnected();
    }

    // This is a hack to resend the message with metainformation
    function _resendLoggedData(dataPoint) {
        _log_debug("Resending message id " + dataPoint.id + " with name: '" + dataPoint.name + "' and data: " + dataPoint.data)

        dataPoint.type <- BULLWINKLE_MESSAGE_TYPE.SEND;

        local package = Bullwinkle.Package(dataPoint)
            .onSuccess(_onSuccess.bindenv(this))
            .onFail(_onFail.bindenv(this));

        if(dataPoint.id in _bullwinkle._packages) //Prevent overwriting of any bullwinkle packages with similair IDs
          dataPoint.id = _bullwinkle._generateId()

        _bullwinkle._packages[dataPoint.id] <- package;
        _bullwinkle._sendMessage(dataPoint);
    }


    function _retry() {
        if(typeof(_next) == "function"){  // We were already in the middle of a read - continue from where we started.
            _next(true);
            _next = null;
        } else {
            _log_debug("Start processing pending messages...");
            _spiFlashLogger.read(
                function(dataPoint, addr, next) {
                    _log_debug("Reading from SPI Flash. ID: " + dataPoint.id + " at addr: " + addr);

                    // There's no point of retrying to send pending messages when disconnected
                    if (!_connectionManager.isConnected()) {
                        _log_debug("No connection, abort SPI Flash scanning...");
                        // Abort scanning
                        next(false);
                        return;
                    }

                    if(time() == IMP_PAGER_RTC_INVALID_TIME){ // If time is invalid, we aren't ready to resend any data just yet...
                        _log_debug("time() was invalid, abort SPI Flash scanning...");
                        next(false);
                        return;
                    }

                    // Don't do any further scanning until we get an ACK for the message we are getting ready to send
                    _next = next

                    // Save SPI Flash address in the message metadata
                    if(!("metadata" in dataPoint)) dataPoint.metadata <- {}
                    dataPoint.metadata.addr <- addr;

                    if("rtc" in dataPoint.metadata && dataPoint.metadata.rtc == false){
                      if(_lastTS == null){
                        _lastTS = [hardware.millis(), time()] //With these two datapoints, we can now re-establish all of our timestamps
                        _log_debug("Discovered most recent datapoint saved to SPIFlash without RTC - " + dataPoint.id + " attempting to rebuild timestamps with ms = " + _lastTS[0] + " and time = " + _lastTS[1])
                      }

                      _log_debug("Found log without RTC. ID=" + dataPoint.id + " and ts=" +dataPoint.ts)

                      if("boot" in dataPoint.metadata && dataPoint.metadata.boot == _bootNumber){
                        local deltaTMillis = _lastTS[0] - dataPoint.ts
                        local deltaTSeconds = math.floor(deltaTMillis+500).tointeger()/1000 //Round to nearest second, but use this int value for updating _lastTS to keep things consistent
                        dataPoint.ts = _lastTS[1] - deltaTSeconds  //All integer math, so no need to worry about decimal points

                        _log_debug("Calculated new ts as " + dataPoint.ts + " (deltaT = " + deltaTMillis + " ms)")

                        //update _lastTS so that we can have 25 days between datapoints instead of 25 days total of timestamps that we can rebuild
                        _lastTS[0] -= (deltaTSeconds*1000)
                        _lastTS[1] = dataPoint.ts

                        // Our RTC has been reset - delete the metadata
                        delete dataPoint.metadata.rtc

                        // Update the dataPoint in our SPIFlash with its proper RTC.
                        local newAddr = _spiFlashLogger.getPosition() + _spiFlashLogger._start;
                        delete dataPoint.metadata.addr
                        _spiFlashLogger.write(dataPoint)
                        dataPoint.metadata.addr <- newAddr
                        _log_debug("Wrote data with updated RTC to addr " + newAddr + " and Deleting datapoint at addr " + addr)

                        // Delete the old dataPoint without the RTC.
                        _spiFlashLogger.erase(addr);

                      } else {
                        server.error("Warning - dataPoint bootNumber " + dpBootNum + " != device bootNumber " + _bootNumber + " for message ID " + dataPoint.id + ".  Unable to rebuild ts...")
                      }
                    }

                    _resendLoggedData(dataPoint);

                }.bindenv(this),

                function() {
                    _log_debug("Finished processing all pending messages");
                }.bindenv(this),

                -1  // Read through the data from most recent (which is important for real-time apps) to oldest, 1 at a time.  This also allows us to rebuild our timestamps from newest to oldest
            );
        }
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
        if (_debug) {
            _connectionManager.log(str);
        }
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

    function onConnect(callback) {
        _onConnectHandlers = _addHandlerAndCleanupEmptyOnes(_onConnectHandlers, callback);
    }

    function onDisconnect(callback) {
        _onDisconnectHandlers = _addHandlerAndCleanupEmptyOnes(_onDisconnectHandlers, callback);
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
};

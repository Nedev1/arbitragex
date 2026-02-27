using System;
using System.Threading;
using System.Threading.Tasks;
using com.omnesys.rapi;

namespace ArbitrageX;

public enum RithmicConnectionState
{
    Disconnected,
    Connecting,
    Connected
}

public sealed class RithmicFeed : IDisposable
{
    private const SubscriptionFlags MarketDataFlags =
        SubscriptionFlags.Best | SubscriptionFlags.Quotes | SubscriptionFlags.Prints;

    private readonly MmfTickPublisher _tickPublisher;
    private readonly object _lifecycleLock = new();

    private REngine? _engine;
    private RithmicCallbacks? _callbacks;

    private int _stateValue = (int)RithmicConnectionState.Disconnected;
    private string _connectionStatus = "Disconnected";

    private string _username = string.Empty;
    private string _password = string.Empty;
    private string _system = string.Empty;
    private string _gateway = string.Empty;
    private string _targetSymbol = string.Empty;

    public RithmicConnectionState ConnectionState => (RithmicConnectionState)Volatile.Read(ref _stateValue);
    public bool IsConnected => ConnectionState == RithmicConnectionState.Connected;

    public string ConnectionStatus
    {
        get
        {
            lock (_lifecycleLock)
            {
                return _connectionStatus;
            }
        }
    }

    public RithmicFeed(MmfTickPublisher tickPublisher)
    {
        _tickPublisher = tickPublisher;
    }

    // Compatibility wrapper for existing UI wiring.
    public Task StartAsync(string username, string password, string system, string gateway, string masterSymbol = "")
    {
        Connect(username, password, system, gateway, masterSymbol);
        return Task.CompletedTask;
    }

    // Compatibility wrapper for existing UI wiring.
    public void Stop()
    {
        Disconnect();
    }

    public void Connect(string username, string password, string system, string gateway, string masterSymbol)
    {
        lock (_lifecycleLock)
        {
            if (ConnectionState != RithmicConnectionState.Disconnected)
            {
                return;
            }

            _username = username;
            _password = password;
            _system = system;
            _gateway = gateway;
            _targetSymbol = masterSymbol;
            SetState(RithmicConnectionState.Connecting, "Initializing REngine...");
        }

        Task.Run(ConnectInternal);
    }

    private void ConnectInternal()
    {
        try
        {
            ParseTargetSymbol(_targetSymbol, out var exchange, out var ticker);
            _callbacks = new RithmicCallbacks(_tickPublisher, ticker);

            var p = new REngineParams
            {
                AppName = "dane:ArbitrageX",
                AppVersion = "1.0",
                DmnSrvrAddr = "ritpz01004.01.rithmic.com:65000~ritpz04063.04.rithmic.com:65000~ritpz01004.01.rithmic.net:65000~ritpz04063.04.rithmic.net:65000~ritpz01004.01.theomne.net:65000~ritpz04063.04.theomne.net:65000~ritpz01004.01.theomne.com:65000~ritpz04063.04.theomne.com:65000",
                DomainName = "rithmic_paper_prod_domain",
                LicSrvrAddr = "ritpz01004.01.rithmic.com:56000~ritpz04063.04.rithmic.com:56000~ritpz01004.01.rithmic.net:56000~ritpz04063.04.rithmic.net:56000~ritpz01004.01.theomne.net:56000~ritpz04063.04.theomne.net:56000~ritpz01004.01.theomne.com:56000~ritpz04063.04.theomne.com:56000",
                LocBrokAddr = "ritpz01004.01.rithmic.com:64100",
                LoggerAddr = "ritpz01004.01.rithmic.com:45454~ritpz04063.04.rithmic.com:45454~ritpz01004.01.rithmic.net:45454~ritpz04063.04.rithmic.net:45454~ritpz01004.01.theomne.net:45454~ritpz04063.04.theomne.net:45454~ritpz01004.01.theomne.com:45454~ritpz04063.04.theomne.com:45454",
                LogFilePath = "rithmic_engine.log",
                UseTraceSource = false
            };

            _engine = new REngine(p);

            // Keep env keys empty as required for this login setup.
            const string envKey = "";
            string safeUser = _username;
            string safePass = _password;

            SetState(RithmicConnectionState.Connecting, "Logging in to Rithmic Paper Trading (Chicago)...");

            _engine.login(
                _callbacks,
                envKey, safeUser, safePass, "login_agent_tp_paperc",
                envKey, safeUser, safePass, "login_agent_op_paperc",
                "login_agent_pnl_paperc",
                envKey, safeUser, safePass, "login_agent_history_paperc");

            SetState(RithmicConnectionState.Connected, "Connected to Rithmic Paper Trading!");

            if (!string.IsNullOrWhiteSpace(ticker))
            {
                _engine.subscribe(exchange, ticker, MarketDataFlags, null);
                AsyncLogger.Enqueue($"[RITHMIC] Subscribed: {exchange}|{ticker} flags={MarketDataFlags}");
            }
        }
        catch (Exception ex)
        {
            SetState(RithmicConnectionState.Disconnected, $"Error: {ex.Message}");
        }
    }

    private static void ParseTargetSymbol(string raw, out string exchange, out string ticker)
    {
        exchange = "CME";
        ticker = (raw ?? string.Empty).Trim();
        if (ticker.Length == 0)
        {
            return;
        }

        if (ticker.Contains("|"))
        {
            var parts = ticker.Split('|');
            if (parts.Length >= 2)
            {
                exchange = (parts[0] ?? string.Empty).Trim();
                ticker = (parts[1] ?? string.Empty).Trim();
            }
        }
        else if (ticker.Contains("."))
        {
            var parts = ticker.Split('.');
            if (parts.Length >= 2)
            {
                ticker = (parts[0] ?? string.Empty).Trim();
                exchange = (parts[1] ?? string.Empty).Trim();
            }
        }

        if (exchange.Length == 0)
        {
            exchange = "CME";
        }
    }

    public void Disconnect()
    {
        lock (_lifecycleLock)
        {
            if (ConnectionState == RithmicConnectionState.Disconnected)
            {
                return;
            }

            SetState(RithmicConnectionState.Disconnected, "Disconnecting...");

            try
            {
                _engine?.logout();
                _engine?.shutdown();
            }
            catch
            {
                // Ignore teardown errors.
            }

            _engine = null;
            _callbacks = null;
        }
    }

    public void Dispose()
    {
        Disconnect();
    }

    private void SetState(RithmicConnectionState state, string status)
    {
        Volatile.Write(ref _stateValue, (int)state);
        lock (_lifecycleLock)
        {
            _connectionStatus = status;
        }

        AsyncLogger.Enqueue($"[RITHMIC] {status}");
    }

    private sealed class RithmicCallbacks : RCallbacks
    {
        private const long MaxBidAskJoinSkewMs = 120;
        private const double MaxAllowedBidAskSpread = 8.0;

        private readonly MmfTickPublisher _tickPublisher;
        private readonly object _stateLock = new();

        private long _callbackBestTicks;
        private long _callbackBidTicks;
        private long _callbackAskTicks;
        private long _callbackQuoteTicks;
        private long _callbackTradeTicks;
        private long _emittedTicks;
        private long _droppedNoSideTicks;
        private long _droppedJoinSkewTicks;
        private long _droppedWideSpreadTicks;
        private long _lastDiagTsMs;

        // Keep last known top-of-book so any callback can emit coherent L1.
        private double _lastBid;
        private double _lastAsk;
        private long _lastBidTsMs;
        private long _lastAskTsMs;
        private string _lastSymbol = "UNKNOWN";

        public RithmicCallbacks(MmfTickPublisher tickPublisher, string configuredTicker)
        {
            _tickPublisher = tickPublisher;
            _lastSymbol = string.IsNullOrWhiteSpace(configuredTicker) ? "UNKNOWN" : configuredTicker;
        }

        public override void BestBidAskQuote(BidInfo oBid, AskInfo oAsk)
        {
            lock (_stateLock)
            {
                _callbackBestTicks++;
                ApplyBid(oBid);
                ApplyAsk(oAsk);
                EmitTickIfReady();
            }
        }

        public override void BestBidQuote(BidInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackBidTicks++;
                ApplyBid(oInfo);
                EmitTickIfReady();
            }
        }

        public override void BestAskQuote(AskInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackAskTicks++;
                ApplyAsk(oInfo);
                EmitTickIfReady();
            }
        }

        public override void BidQuote(BidInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackBidTicks++;
                // Non-best quote updates can include depth changes; do not re-anchor L1 from them.
                EmitDiagnosticsIfDue(NowMs());
            }
        }

        public override void AskQuote(AskInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackAskTicks++;
                // Non-best quote updates can include depth changes; do not re-anchor L1 from them.
                EmitDiagnosticsIfDue(NowMs());
            }
        }

        public override void Quote(QuoteInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackQuoteTicks++;
                if (oInfo != null)
                {
                    long nowMs = NowMs();
                    if (oInfo.Bid && oInfo.BidPriceToFill > 0.0)
                    {
                        _lastBid = oInfo.BidPriceToFill;
                        _lastBidTsMs = nowMs;
                    }

                    if (oInfo.Ask && oInfo.AskPriceToFill > 0.0)
                    {
                        _lastAsk = oInfo.AskPriceToFill;
                        _lastAskTsMs = nowMs;
                    }

                    if (!string.IsNullOrWhiteSpace(oInfo.Symbol))
                    {
                        _lastSymbol = oInfo.Symbol;
                    }
                }

                EmitTickIfReady();
            }
        }

        public override void TradePrint(TradeInfo oInfo)
        {
            lock (_stateLock)
            {
                _callbackTradeTicks++;
                if (oInfo != null && !string.IsNullOrWhiteSpace(oInfo.Symbol))
                {
                    _lastSymbol = oInfo.Symbol;
                }

                EmitDiagnosticsIfDue(NowMs());
            }
        }

        public override void Alert(AlertInfo pInfo)
        {
            AsyncLogger.Enqueue($"[RITHMIC ALERT] {pInfo.Message}");
        }

        private static long NowMs()
        {
            return (long)(System.Diagnostics.Stopwatch.GetTimestamp() * 1000.0 / System.Diagnostics.Stopwatch.Frequency);
        }

        private void ApplyBid(BidInfo info)
        {
            if (info == null)
            {
                return;
            }

            if (info.Price > 0.0)
            {
                _lastBid = info.Price;
                _lastBidTsMs = NowMs();
            }

            if (!string.IsNullOrWhiteSpace(info.Symbol))
            {
                _lastSymbol = info.Symbol;
            }
        }

        private void ApplyAsk(AskInfo info)
        {
            if (info == null)
            {
                return;
            }

            if (info.Price > 0.0)
            {
                _lastAsk = info.Price;
                _lastAskTsMs = NowMs();
            }

            if (!string.IsNullOrWhiteSpace(info.Symbol))
            {
                _lastSymbol = info.Symbol;
            }
        }

        private void EmitTickIfReady()
        {
            long ts = NowMs();
            if (_lastBid <= 0.0 || _lastAsk <= 0.0)
            {
                _droppedNoSideTicks++;
                EmitDiagnosticsIfDue(ts);
                return;
            }

            if (_lastBidTsMs <= 0 || _lastAskTsMs <= 0 || Math.Abs(_lastBidTsMs - _lastAskTsMs) > MaxBidAskJoinSkewMs)
            {
                _droppedJoinSkewTicks++;
                EmitDiagnosticsIfDue(ts);
                return;
            }

            var spread = _lastAsk - _lastBid;
            if (spread <= 0.0 || spread > MaxAllowedBidAskSpread)
            {
                _droppedWideSpreadTicks++;
                EmitDiagnosticsIfDue(ts);
                return;
            }

            _tickPublisher.PublishMasterTick(_lastBid, _lastAsk, ts);
            _emittedTicks++;
            EmitDiagnosticsIfDue(ts);
        }

        private void EmitDiagnosticsIfDue(long ts)
        {
            if (ts - _lastDiagTsMs < 5000)
            {
                return;
            }

            long elapsedMs = _lastDiagTsMs > 0 ? (ts - _lastDiagTsMs) : 5000;
            if (elapsedMs <= 0)
            {
                elapsedMs = 5000;
            }

            double emittedTps = _emittedTicks * 1000.0 / elapsedMs;
            AsyncLogger.Enqueue(
                $"[RITHMIC RAW] cb(best={_callbackBestTicks},bid={_callbackBidTicks},ask={_callbackAskTicks},quote={_callbackQuoteTicks},print={_callbackTradeTicks}) " +
                $"emit={_emittedTicks} dropNoSide={_droppedNoSideTicks} dropJoin={_droppedJoinSkewTicks} dropWide={_droppedWideSpreadTicks} " +
                $"tps={emittedTps:F2} symbol={_lastSymbol}");

            _callbackBestTicks = 0;
            _callbackBidTicks = 0;
            _callbackAskTicks = 0;
            _callbackQuoteTicks = 0;
            _callbackTradeTicks = 0;
            _emittedTicks = 0;
            _droppedNoSideTicks = 0;
            _droppedJoinSkewTicks = 0;
            _droppedWideSpreadTicks = 0;
            _lastDiagTsMs = ts;
        }
    }
}

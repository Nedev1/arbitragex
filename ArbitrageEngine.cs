using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArbitrageX;

public sealed class ArbitrageEngine
{
    private const long _maxExecutionLatencyMs = 150;
    private const long _maxEntryAckWaitMs = 20000;
    private const long _fillResponseTimeoutMs = 2000;
    private const long _masterFeedStaleMs = 1500;
    private const long _masterStaleWarningIntervalMs = 2000;
    private const int _unstableTickLimit = 250;
    private const string _analysisSourceTerminalId = "RITHMIC-FAST";
    private const string _cmdKillAll = "KILL|ALL";
    private const string _cmdTradePrefix = "TRADE|";
    private const string _cmdModifyPrefix = "MODIFY|";
    private const string _cmdKillPrefix = "KILL|";

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentThread();

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern UIntPtr SetThreadAffinityMask(IntPtr hThread, UIntPtr dwThreadAffinityMask);

    private readonly TickRingBuffer _tickBuffer;
    private readonly PipeServer _pipes;
    private Thread? _thread;
    private CancellationTokenSource? _cts;

    private readonly int _calibrationWindow = 100;
    private readonly double[] _offsetWindow;
    private int _offsetIndex;
    private int _offsetCount;
    private double _offsetSum;
    private int _unstableTickCount;

    private double _lotSize = 0.1;
    private double _maxSpread = 2.0;
    private double _triggerThreshold = 0.5;
    private double _hardStopLoss = 5.0;
    private string _sourceTerminal = string.Empty;
    private string _targetTerminal = string.Empty;
    private string _masterSymbol = string.Empty;
    private string _slaveSymbol = string.Empty;
    private volatile bool _tradingEnabled;
    private volatile bool _hasOpenPosition;
    private long _positionOpenTimeMs;
    private double _openPrice;
    private int _openSide; // 1 = Buy, -1 = Sell, 0 = None
    private long _requestTimeMs;
    private long _lastOrderSentTimeMs;
    private double _requestPrice;
    private double _entryGapDistance;
    private double _entryStopDistance;
    private double _targetTpPrice;
    private double _targetSlPrice;
    private volatile bool _isAwaitingFill;
    private long _openTicket;
    private long _lastFlatTimeMs;
    private long _rejectSpreadCount;
    private long _rejectThresholdCount;
    private string _activeTargetTerminal = string.Empty;
    private string _activeSlaveSymbol = string.Empty;
    private int _sourceTerminalHash;
    private int _targetTerminalHash;
    private int _masterSymbolHash;
    private int _slaveSymbolHash;

    private double _srcBid;
    private double _srcAsk;
    private long _srcTs;
    private long _lastMasterTickMs;
    private long _lastMasterStaleWarningMs;
    private double _tgtBid;
    private double _tgtAsk;
    private long _tgtTs;
    private bool _hasSrc;
    private bool _hasTgt;

    private long _ticksThisSecond;
    private long _lastSecondTimestamp;
    private long _sourceTicksSeen;
    private long _targetTicksSeen;
    private double _slippageAbsEwma;
    private double _latencyMsEwma;
    private int _execQualitySamples;

    private readonly object _configLock = new();

    // Analysis state
    private readonly object _analysisLock = new();
    private bool _isAnalyzing;
    private DateTime? _analysisEndUtc;
    private TaskCompletionSource<AnalysisResult>? _analysisTcs;
    private CancellationTokenSource? _analysisCts;
    private readonly List<AnalysisSample> _analysisSamples = new(4096);
    private long _analysisSourceTicks;
    private long _analysisTargetTicks;
    private long _analysisRejectedTicks;
    private long _analysisLatencyMatches;

    public bool IsAnalyzing => _isAnalyzing;
    public DateTime? AnalysisEndUtc => _analysisEndUtc;
    public double CurrentGap { get; private set; }
    public long CurrentTps { get; private set; }
    public bool IsEngineCalibrating { get; private set; }
    public bool TradingEnabled => _tradingEnabled;

    public event Action<FilledMessage>? FillReceived;
    public event Action<string>? Log;

    public ArbitrageEngine(PipeServer pipes, ChannelReader<FilledMessage> fills)
    {
        _pipes = pipes;
        _tickBuffer = _pipes.Ticks;
        _offsetWindow = new double[_calibrationWindow];
        _pipes.PositionClosed += OnPositionClosed;
        _ = Task.Run(() => FillLoop(fills));
    }

    public void Start()
    {
        if (_thread is { IsAlive: true })
        {
            return;
        }

        if (_thread is { IsAlive: false })
        {
            _thread = null;
        }

        _cts?.Cancel();
        _tradingEnabled = false;
        lock (_configLock)
        {
            _sourceTerminalHash = HftHash.GetHash(_sourceTerminal);
            _targetTerminalHash = HftHash.GetHash(_targetTerminal);
            _masterSymbolHash = HftHash.GetSymbolHash(_masterSymbol);
            _slaveSymbolHash = HftHash.GetSymbolHash(_slaveSymbol);
        }
        WarmupJit();
        _cts = new CancellationTokenSource();
        _tradingEnabled = true;

        _thread = new Thread(() => Loop(_cts.Token))
        {
            IsBackground = true,
            Name = "ArbitrageEngine",
            Priority = ThreadPriority.AboveNormal
        };

        _thread.Start();
    }

    private void WarmupJit()
    {
        Log?.Invoke("[ENGINE] Warming up JIT compiler...");
        var sw = Stopwatch.StartNew();

        // Dummy data that won't trigger an actual trade because trading isn't enabled yet
        var fakeMasterTick = new TickData { TerminalHash = _sourceTerminalHash, SymbolHash = _masterSymbolHash, Bid = 10000, Ask = 10001, Timestamp = 1 };
        var fakeSlaveTick = new TickData { TerminalHash = _targetTerminalHash, SymbolHash = _slaveSymbolHash, Bid = 10000, Ask = 10001, Timestamp = 1 };

        // Run hot path
        ProcessTick(fakeMasterTick, sw);
        ProcessTick(fakeSlaveTick, sw);

        // RESET STATE: Clear dummy data so it doesn't affect real gap calculations
        _srcBid = 0;
        _srcAsk = 0;
        _tgtBid = 0;
        _tgtAsk = 0;
        _hasSrc = false;
        _hasTgt = false;
        ResetOpenPositionState();

        _offsetCount = 0;
        _offsetSum = 0;
        _offsetIndex = 0;
        _unstableTickCount = 0;
        ResetEntryPersistence();
        _rejectSpreadCount = 0;
        _rejectThresholdCount = 0;
        _sourceTicksSeen = 0;
        _targetTicksSeen = 0;
        _lastFlatTimeMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        _lastMasterTickMs = 0;
        _lastMasterStaleWarningMs = 0;
        _isAwaitingFill = false;
        _slippageAbsEwma = 0.0;
        _latencyMsEwma = 0.0;
        _execQualitySamples = 0;
        IsEngineCalibrating = false;
        if (_offsetWindow != null)
        {
            Array.Clear(_offsetWindow, 0, _offsetWindow.Length);
        }

        Log?.Invoke("[ENGINE] JIT Warmup complete. State reset. Awaiting live data...");
    }

    public void Stop()
    {
        _tradingEnabled = false;
        if (_cts is null) return;

        _cts.Cancel();
        _thread?.Join(200);
        _thread = null;
        _cts = null;
        CurrentTps = 0;
        CurrentGap = 0;
        IsEngineCalibrating = false;
    }

    public void KillSwitch()
    {
        _tradingEnabled = false;
        _pipes.PublishCommand(_cmdKillAll);
    }

    public void UpdateConfig(EngineConfig config)
    {
        lock (_configLock)
        {
            _lotSize = config.LotSize;
            _maxSpread = config.MaxSpread;
            _triggerThreshold = config.TriggerThreshold;
            _hardStopLoss = config.HardStopLoss;
            _sourceTerminal = (config.SourceTerminal ?? string.Empty).Trim();
            _targetTerminal = (config.TargetTerminal ?? string.Empty).Trim();
            _masterSymbol = (config.MasterSymbol ?? string.Empty).Trim();
            _slaveSymbol = (config.SlaveSymbol ?? string.Empty).Trim();
            _sourceTerminalHash = HftHash.GetHash(_sourceTerminal);
            _targetTerminalHash = HftHash.GetHash(_targetTerminal);
            _masterSymbolHash = HftHash.GetSymbolHash(_masterSymbol);
            _slaveSymbolHash = HftHash.GetSymbolHash(_slaveSymbol);

            _hasSrc = false;
            _hasTgt = false;
            _unstableTickCount = 0;
            _lastMasterTickMs = 0;
            _lastMasterStaleWarningMs = 0;
            _isAwaitingFill = false;
            ResetEntryPersistence();
            IsEngineCalibrating = false;
        }
    }

    public Task<AnalysisResult> StartAnalysis(TimeSpan duration)
    {
        lock (_analysisLock)
        {
            if (_isAnalyzing)
            {
                return _analysisTcs?.Task ?? Task.FromResult(AnalysisResult.Empty("Analysis already running."));
            }

            _analysisSamples.Clear();
            _analysisSourceTicks = 0;
            _analysisTargetTicks = 0;
            _analysisRejectedTicks = 0;
            _analysisLatencyMatches = 0;
            _analysisTcs = new TaskCompletionSource<AnalysisResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _analysisCts?.Cancel();
            _analysisCts = new CancellationTokenSource();

            lock (_configLock)
            {
                _hasSrc = false;
                _hasTgt = false;
                _unstableTickCount = 0;
            }

            var effectiveDuration = duration <= TimeSpan.Zero ? TimeSpan.FromMinutes(1) : duration;
            if (effectiveDuration < TimeSpan.FromSeconds(5))
            {
                effectiveDuration = TimeSpan.FromSeconds(5);
            }

            _isAnalyzing = true;
            _analysisEndUtc = DateTime.UtcNow.Add(effectiveDuration);

            _ = Task.Run(() => AnalyzeAsync(_analysisCts.Token, effectiveDuration));

            return _analysisTcs.Task;
        }
    }

    private void AnalyzeAsync(CancellationToken token, TimeSpan duration)
    {
        string masterTicker;
        string targetTerminal;
        string slaveSymbol;
        lock (_configLock)
        {
            masterTicker = NormalizeMasterTicker(_masterSymbol);
            targetTerminal = string.IsNullOrWhiteSpace(_targetTerminal) ? "MT5-01" : _targetTerminal.Trim();
            slaveSymbol = string.IsNullOrWhiteSpace(_slaveSymbol) ? "NAS100" : _slaveSymbol.Trim();
        }

        int masterSrcHash = HftHash.GetHash(_analysisSourceTerminalId);
        int masterSymHash = HftHash.GetSymbolHash(masterTicker);
        int slaveSrcHash = HftHash.GetHash(targetTerminal);
        int slaveSymHash = HftHash.GetSymbolHash(slaveSymbol);

        double lastRithmicAsk = 0.0;
        double lastMt5Ask = 0.0;
        int pendingDirection = 0;
        long rithmicMoveTimestamp = 0;

        double latestMasterBid = 0.0;
        double latestMasterAsk = 0.0;
        double latestSlaveBid = 0.0;
        double latestSlaveAsk = 0.0;
        bool hasMaster = false;
        bool hasSlave = false;

        double gapSum = 0.0;
        int gapCount = 0;
        double spreadSum = 0.0;
        double maxGap = 0.0;
        double peakSpike = 0.0;
        int winCount = 0;
        double latencySum = 0.0;
        long latencyMatches = 0;
        long sourceTicks = 0;
        long targetTicks = 0;
        long rejectedTicks = 0;

        var localSamples = new List<AnalysisSample>(8192);
        var sw = Stopwatch.StartNew();

        var durationMs = (long)duration.TotalMilliseconds;
        while (!token.IsCancellationRequested && sw.ElapsedMilliseconds < durationMs)
        {
            bool any = false;
            while (_tickBuffer.TryRead(out var tick))
            {
                any = true;

                if (tick.TerminalHash == masterSrcHash && tick.SymbolHash == masterSymHash)
                {
                    sourceTicks++;
                    latestMasterBid = tick.Bid;
                    latestMasterAsk = tick.Ask;
                    hasMaster = latestMasterAsk > 0.0;

                    if (latestMasterAsk > 0.0)
                    {
                        if (lastRithmicAsk > 0.0 && latestMasterAsk != lastRithmicAsk)
                        {
                            pendingDirection = latestMasterAsk > lastRithmicAsk ? 1 : -1;
                            rithmicMoveTimestamp = tick.Timestamp;
                        }

                        lastRithmicAsk = latestMasterAsk;
                    }
                }
                else if (tick.TerminalHash == slaveSrcHash && tick.SymbolHash == slaveSymHash)
                {
                    targetTicks++;
                    latestSlaveBid = tick.Bid;
                    latestSlaveAsk = tick.Ask;
                    hasSlave = latestSlaveAsk > 0.0 && latestSlaveBid > 0.0;

                    if (latestSlaveAsk > 0.0)
                    {
                        if (lastMt5Ask > 0.0 && latestSlaveAsk != lastMt5Ask)
                        {
                            int slaveDirection = latestSlaveAsk > lastMt5Ask ? 1 : -1;
                            if (rithmicMoveTimestamp > 0 && pendingDirection != 0 && slaveDirection == pendingDirection && tick.Timestamp >= rithmicMoveTimestamp)
                            {
                                var latency = ComputeLatencyMilliseconds(rithmicMoveTimestamp, tick.Timestamp);
                                latencySum += latency;
                                latencyMatches++;
                                rithmicMoveTimestamp = 0;
                                pendingDirection = 0;
                            }
                        }

                        lastMt5Ask = latestSlaveAsk;
                    }
                }
                else
                {
                    rejectedTicks++;
                }

                if (hasMaster && hasSlave && latestMasterAsk > 0.0 && latestSlaveAsk > 0.0)
                {
                    var instantGap = latestSlaveAsk - latestMasterAsk;
                    gapSum += instantGap;
                    gapCount++;
                    var staticOffset = gapSum / gapCount;
                    var deviation = instantGap - staticOffset;
                    var spread = latestSlaveAsk - latestSlaveBid;
                    if (spread < 0.0) spread = 0.0;
                    spreadSum += spread;
                    if (Math.Abs(instantGap) > maxGap) maxGap = Math.Abs(instantGap);
                    if (Math.Abs(deviation) > peakSpike) peakSpike = Math.Abs(deviation);
                    if (Math.Abs(instantGap) > spread) winCount++;
                    localSamples.Add(new AnalysisSample(instantGap, deviation, -1.0, spread));
                }
            }

            if (!any)
            {
                Thread.SpinWait(10);
            }
            else
            {
                Thread.Sleep(0);
            }
        }

        AnalysisResult result;
        if (sourceTicks == 0 || targetTicks == 0 || gapCount == 0)
        {
            result = AnalysisResult.Empty(
                $"No ticks detected. SourceTicks={sourceTicks}, TargetTicks={targetTicks}, Rejected={rejectedTicks}. " +
                $"Hashes src={masterSrcHash}/{masterSymHash} tgt={slaveSrcHash}/{slaveSymHash}. " +
                $"Check symbols and market hours.");
        }
        else
        {
            var staticOffset = gapSum / gapCount;
            var avgLatency = latencyMatches > 0 ? (latencySum / latencyMatches) : 0.0;
            var avgSpread = spreadSum / gapCount;
            var winRate = (double)winCount / gapCount;
            result = new AnalysisResult(
                staticOffset,
                maxGap,
                staticOffset,
                peakSpike,
                avgLatency,
                avgSpread,
                winRate,
                gapCount,
                latencyMatches > 0 ? $"OK (Pairs={latencyMatches})" : "OK (No matched direction-latency events)");
        }

        lock (_analysisLock)
        {
            _analysisSamples.Clear();
            _analysisSamples.AddRange(localSamples);
            _analysisSourceTicks = sourceTicks;
            _analysisTargetTicks = targetTicks;
            _analysisRejectedTicks = rejectedTicks;
            _analysisLatencyMatches = latencyMatches;
            _isAnalyzing = false;
            _analysisEndUtc = null;
        }

        _analysisTcs?.TrySetResult(result);
    }

    private void FinishAnalysis()
    {
        AnalysisResult result;
        lock (_analysisLock)
        {
            if (!_isAnalyzing)
            {
                return;
            }

            if (_analysisSourceTicks == 0 || _analysisTargetTicks == 0)
            {
                var status =
                    $"No analysis ticks detected. SourceTicks={_analysisSourceTicks}, TargetTicks={_analysisTargetTicks}, Rejected={_analysisRejectedTicks}. Check RITHMIC-FAST/{NormalizeMasterTicker(_masterSymbol)} and {_targetTerminal}/{_slaveSymbol}.";
                result = AnalysisResult.Empty(status);
            }
            else if (_analysisSamples.Count == 0)
            {
                var status =
                    $"No paired analysis samples captured. SourceTicks={_analysisSourceTicks}, TargetTicks={_analysisTargetTicks}, Rejected={_analysisRejectedTicks}.";
                result = AnalysisResult.Empty(status);
            }
            else
            {
                double gapSum = 0;
                double latencySum = 0;
                double spreadSum = 0;
                int winCount = 0;
                double maxGap = 0;

                foreach (var s in _analysisSamples)
                {
                    gapSum += s.Gap;
                    if (s.LatencyMs >= 0.0)
                    {
                        latencySum += s.LatencyMs;
                    }
                    spreadSum += s.Spread;
                    var absGap = Math.Abs(s.Gap);
                    if (absGap > maxGap) maxGap = absGap;
                    if (Math.Abs(s.Gap) > s.Spread) winCount++;
                }

                var n = _analysisSamples.Count;
                var staticOffset = gapSum / n;
                var peakSpike = 0.0;
                foreach (var s in _analysisSamples)
                {
                    var absDev = Math.Abs(s.Gap - staticOffset);
                    if (absDev > peakSpike) peakSpike = absDev;
                }

                var avgLatency = _analysisLatencyMatches > 0 ? (latencySum / _analysisLatencyMatches) : 0.0;
                result = new AnalysisResult(
                    gapSum / n,
                    maxGap,
                    staticOffset,
                    peakSpike,
                    avgLatency,
                    spreadSum / n,
                    (double)winCount / n,
                    n,
                    _analysisLatencyMatches > 0
                        ? $"OK (Pairs={_analysisLatencyMatches})"
                        : "OK (No matched direction-latency events)");
            }

            _isAnalyzing = false;
            _analysisEndUtc = null;
        }

        _analysisTcs?.TrySetResult(result);
    }

    private void Loop(CancellationToken token)
    {
        try
        {
            // Pin thread to CPU Core 2 (mask = 1 << 2 = 4)
            SetThreadAffinityMask(GetCurrentThread(), new UIntPtr(4));
            Thread.BeginThreadAffinity();
        }
        catch
        {
            // Ignore if permissions fail
        }

        var sw = Stopwatch.StartNew();
        _lastSecondTimestamp = sw.ElapsedMilliseconds;
        var spin = new SpinWait();

        while (!token.IsCancellationRequested)
        {
            try
            {
                CheckAwaitingFillTimeout();

                if (_isAnalyzing)
                {
                    Thread.Sleep(0);
                    continue;
                }

                var any = false;
                while (_tickBuffer.TryRead(out var tick))
                {
                    any = true;
                    ProcessTick(tick, sw);
                }

                UpdateTpsClock(sw);

                if (!any)
                {
                    spin.SpinOnce();
                }
                else
                {
                    spin.Reset();
                }
            }
            catch (Exception ex)
            {
                Log?.Invoke($"[ENGINE CRASH PREVENTED] {ex.Message}");
                Thread.Sleep(10);
            }
        }
    }

    private void UpdateTpsClock(Stopwatch sw)
    {
        var nowMs = sw.ElapsedMilliseconds;
        var elapsedMs = nowMs - _lastSecondTimestamp;
        if (elapsedMs < 1000)
        {
            return;
        }

        var elapsedSeconds = elapsedMs / 1000.0;
        CurrentTps = (long)(Interlocked.Exchange(ref _ticksThisSecond, 0) / elapsedSeconds);
        _lastSecondTimestamp = nowMs;
    }

    private void CheckAwaitingFillTimeout()
    {
        if (!_isAwaitingFill)
        {
            return;
        }

        var lastOrderSentMs = _lastOrderSentTimeMs;
        if (lastOrderSentMs <= 0)
        {
            _isAwaitingFill = false;
            return;
        }

        var nowMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        var elapsedMs = nowMs - lastOrderSentMs;
        if (elapsedMs <= _fillResponseTimeoutMs)
        {
            return;
        }

        _isAwaitingFill = false;
        ResetOpenPositionState();
        Log?.Invoke("[TIMEOUT] Fill response lost. Resetting state.");
    }

    private void ProcessTick(TickData tick, Stopwatch sw)
    {
        string targetTerminal;
        string slaveSymbol;
        double lotSize;
        double maxSpread;
        double triggerThreshold;
        double hardStopLoss;

        lock (_configLock)
        {
            targetTerminal = _targetTerminal;
            slaveSymbol = _slaveSymbol;
            lotSize = _lotSize;
            maxSpread = _maxSpread;
            triggerThreshold = _triggerThreshold;
            hardStopLoss = _hardStopLoss;
        }

        if (targetTerminal.Length == 0)
        {
            return;
        }

        var nowMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        var isSource = tick.TerminalHash == _sourceTerminalHash && tick.SymbolHash == _masterSymbolHash;
        var isTarget = tick.TerminalHash == _targetTerminalHash && tick.SymbolHash == _slaveSymbolHash;

        if (!isSource && !isTarget)
        {
            if (_isAnalyzing)
            {
                Interlocked.Increment(ref _analysisRejectedTicks);
            }
            return;
        }

        if (isSource)
        {
            _sourceTicksSeen++;
            _srcBid = tick.Bid;
            _srcAsk = tick.Ask;
            _srcTs = tick.Timestamp;
            _lastMasterTickMs = nowMs;
            _lastMasterStaleWarningMs = 0;
            _hasSrc = true;
            if (_isAnalyzing)
            {
                Interlocked.Increment(ref _analysisSourceTicks);
            }
        }
        else
        {
            _targetTicksSeen++;
            _tgtBid = tick.Bid;
            _tgtAsk = tick.Ask;
            _tgtTs = tick.Timestamp;
            _hasTgt = true;
            if (_isAnalyzing)
            {
                Interlocked.Increment(ref _analysisTargetTicks);
            }
        }

        _ticksThisSecond++;

        if (!_hasSrc || !_hasTgt)
        {
            IsEngineCalibrating = false;
            return;
        }

        if (!isSource && _lastMasterTickMs > 0)
        {
            var masterAgeMs = nowMs - _lastMasterTickMs;
            if (masterAgeMs > _masterFeedStaleMs)
            {
                if (nowMs - _lastMasterStaleWarningMs >= _masterStaleWarningIntervalMs)
                {
                    _lastMasterStaleWarningMs = nowMs;
                    Log?.Invoke(string.Concat(
                        "[WARN] Master feed stale (age=",
                        masterAgeMs.ToString(CultureInfo.InvariantCulture),
                        "ms). Entry evaluation paused."));
                }

                IsEngineCalibrating = false;
                return;
            }
        }

        var sourceMid = (_srcBid + _srcAsk) * 0.5;
        var targetMid = (_tgtBid + _tgtAsk) * 0.5;
        var instantaneousOffset = targetMid - sourceMid;
        var targetSpread = _tgtAsk - _tgtBid;
        if (targetSpread < 0)
        {
            targetSpread = 0;
        }

        if (_offsetCount < _calibrationWindow)
        {
            _offsetWindow[_offsetCount] = instantaneousOffset;
            _offsetSum += instantaneousOffset;
            _offsetCount++;
            if (_offsetCount == _calibrationWindow)
            {
                _offsetIndex = 0;
            }

            CurrentGap = instantaneousOffset;
            IsEngineCalibrating = true;
            return;
        }

        var averageOffset = _offsetSum / _calibrationWindow;
        var deviation = instantaneousOffset - averageOffset;
        var spikeProtectThreshold = triggerThreshold > 0.0 ? (triggerThreshold * 0.5) : double.MaxValue;
        var isStableForBaseline = Math.Abs(deviation) < spikeProtectThreshold;

        // Keep baseline static while position is open; refresh only when flat.
        if (!_hasOpenPosition && isStableForBaseline)
        {
            _unstableTickCount = 0;
            _offsetSum -= _offsetWindow[_offsetIndex];
            _offsetWindow[_offsetIndex] = instantaneousOffset;
            _offsetSum += instantaneousOffset;
            _offsetIndex++;
            if (_offsetIndex >= _calibrationWindow)
            {
                _offsetIndex = 0;
            }
        }
        else if (!_hasOpenPosition)
        {
            _unstableTickCount++;
            if (_unstableTickCount >= _unstableTickLimit)
            {
                for (var i = 0; i < _calibrationWindow; i++)
                {
                    _offsetWindow[i] = instantaneousOffset;
                }

                _offsetSum = instantaneousOffset * _calibrationWindow;
                _offsetCount = _calibrationWindow;
                _offsetIndex = 0;
                _unstableTickCount = 0;
                averageOffset = instantaneousOffset;
                deviation = 0.0;
            }
        }

        averageOffset = _offsetSum / _calibrationWindow;
        deviation = instantaneousOffset - averageOffset;

        var mappedSourceBid = _srcBid + averageOffset;
        var mappedSourceAsk = _srcAsk + averageOffset;
        var edgeBuy = mappedSourceBid - _tgtAsk;
        var edgeSell = _tgtBid - mappedSourceAsk;

        CurrentGap = deviation;
        IsEngineCalibrating = false;

        if (_hasOpenPosition)
        {
            if (_requestTimeMs > 0)
            {
                var entryPendingMs = nowMs - _requestTimeMs;
                if (entryPendingMs > _maxEntryAckWaitMs)
                {
                    var killTerminal = _activeTargetTerminal.Length > 0 ? _activeTargetTerminal : targetTerminal;
                    var killSymbol = _activeSlaveSymbol.Length > 0 ? _activeSlaveSymbol : slaveSymbol;
                    _pipes.PublishCommand(BuildKillCommand(killTerminal, killSymbol));
                    Log?.Invoke(string.Concat(
                        "[ENGINE] Entry ack timeout (",
                        entryPendingMs.ToString(CultureInfo.InvariantCulture),
                        "ms). Sent KILL and applied failsafe state reset."));
                    _lastFlatTimeMs = nowMs;
                    ResetOpenPositionState();
                    return;
                }
            }

            // While position is open, keep monitoring gap and do not open new entries.
            return;
        }

        if (!_tradingEnabled)
        {
            return;
        }

        if (targetSpread > maxSpread)
        {
            _rejectSpreadCount++;
            return;
        }

        var effectiveTrigger = triggerThreshold;
        var buyCandidate = edgeBuy >= effectiveTrigger;
        var sellCandidate = edgeSell >= effectiveTrigger;

        if (!buyCandidate && !sellCandidate)
        {
            _rejectThresholdCount++;
            return;
        }

        if (buyCandidate && (!sellCandidate || edgeBuy >= edgeSell))
        {
            var targetTp = _tgtAsk + edgeBuy;
            var targetSl = hardStopLoss > 0.0 ? (_tgtAsk - hardStopLoss) : 0.0;

            _hasOpenPosition = true;
            _positionOpenTimeMs = sw.ElapsedMilliseconds;
            _openSide = 1;
            var orderSentMonoMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
            var orderSentUtcMs = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _requestTimeMs = orderSentMonoMs;
            _lastOrderSentTimeMs = orderSentMonoMs;
            _isAwaitingFill = true;
            _requestPrice = _tgtAsk;
            _entryGapDistance = edgeBuy;
            _entryStopDistance = hardStopLoss > 0.0 ? hardStopLoss : 0.0;
            _openPrice = _requestPrice;
            _targetTpPrice = targetTp;
            _targetSlPrice = targetSl;
            _openTicket = 0;
            _activeTargetTerminal = targetTerminal;
            _activeSlaveSymbol = slaveSymbol;
            Log?.Invoke(string.Concat(
                "[TRADE] BUY edge=", edgeBuy.ToString("F5", CultureInfo.InvariantCulture),
                " dev=", deviation.ToString("F5", CultureInfo.InvariantCulture),
                " spread=", targetSpread.ToString("F5", CultureInfo.InvariantCulture),
                " offset=", averageOffset.ToString("F5", CultureInfo.InvariantCulture),
                " trig=", effectiveTrigger.ToString("F5", CultureInfo.InvariantCulture),
                " TP=", targetTp.ToString("F5", CultureInfo.InvariantCulture),
                " SL=", targetSl.ToString("F5", CultureInfo.InvariantCulture)));
            _pipes.PublishCommand(BuildTradeCommand("BUY", slaveSymbol, lotSize, targetSl, targetTp, targetTerminal, orderSentUtcMs));
            return;
        }

        var sellTp = _tgtBid - edgeSell;
        var sellSl = hardStopLoss > 0.0 ? (_tgtBid + hardStopLoss) : 0.0;

        _hasOpenPosition = true;
        _positionOpenTimeMs = sw.ElapsedMilliseconds;
        _openSide = -1;
        var sellOrderSentMonoMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        var sellOrderSentUtcMs = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _requestTimeMs = sellOrderSentMonoMs;
        _lastOrderSentTimeMs = sellOrderSentMonoMs;
        _isAwaitingFill = true;
        _requestPrice = _tgtBid;
        _entryGapDistance = edgeSell;
        _entryStopDistance = hardStopLoss > 0.0 ? hardStopLoss : 0.0;
        _openPrice = _requestPrice;
        _targetTpPrice = sellTp;
        _targetSlPrice = sellSl;
        _openTicket = 0;
        _activeTargetTerminal = targetTerminal;
        _activeSlaveSymbol = slaveSymbol;
        Log?.Invoke(string.Concat(
            "[TRADE] SELL edge=", edgeSell.ToString("F5", CultureInfo.InvariantCulture),
            " dev=", deviation.ToString("F5", CultureInfo.InvariantCulture),
            " spread=", targetSpread.ToString("F5", CultureInfo.InvariantCulture),
            " offset=", averageOffset.ToString("F5", CultureInfo.InvariantCulture),
            " trig=", effectiveTrigger.ToString("F5", CultureInfo.InvariantCulture),
            " TP=", sellTp.ToString("F5", CultureInfo.InvariantCulture),
            " SL=", sellSl.ToString("F5", CultureInfo.InvariantCulture)));
        _pipes.PublishCommand(BuildTradeCommand("SELL", slaveSymbol, lotSize, sellSl, sellTp, targetTerminal, sellOrderSentUtcMs));
    }

    private static double ComputeLatencyMilliseconds(long sourceTimestamp, long targetTimestamp)
    {
        if (sourceTimestamp <= 0 || targetTimestamp <= 0)
        {
            return 0.0;
        }

        var directMs = (double)(targetTimestamp - sourceTimestamp);
        var candidateTargetUs = (targetTimestamp / 1000.0) - sourceTimestamp;
        var candidateSourceUs = targetTimestamp - (sourceTimestamp / 1000.0);

        // Pick the most plausible latency candidate (smallest absolute magnitude).
        var best = directMs;
        if (Math.Abs(candidateTargetUs) < Math.Abs(best)) best = candidateTargetUs;
        if (Math.Abs(candidateSourceUs) < Math.Abs(best)) best = candidateSourceUs;
        if (best < 0.0) best = 0.0;
        return best;
    }

    private static string NormalizeMasterTicker(string masterSymbol)
    {
        if (string.IsNullOrWhiteSpace(masterSymbol))
        {
            return string.Empty;
        }

        var ticker = masterSymbol.Trim();
        var pipeIdx = ticker.IndexOf('|');
        if (pipeIdx >= 0 && pipeIdx + 1 < ticker.Length)
        {
            ticker = ticker.Substring(pipeIdx + 1).Trim();
        }

        var dotIdx = ticker.IndexOf('.');
        if (dotIdx > 0)
        {
            ticker = ticker.Substring(0, dotIdx).Trim();
        }

        return ticker;
    }

    private async Task FillLoop(ChannelReader<FilledMessage> fills)
    {
        while (await fills.WaitToReadAsync().ConfigureAwait(false))
        {
            while (fills.TryRead(out var fill))
            {
                var isOrderError = string.Equals(fill.Side, "OrderFailed", StringComparison.OrdinalIgnoreCase);
                if (isOrderError)
                {
                    if (_isAwaitingFill)
                    {
                        var expectedTerminal = _activeTargetTerminal;
                        var expectedSymbol = _activeSlaveSymbol;
                        var fillTerminal = fill.TerminalId ?? string.Empty;
                        var isExpectedTerminal = expectedTerminal.Length == 0
                                                 || string.Equals(fillTerminal, expectedTerminal, StringComparison.OrdinalIgnoreCase);
                        var isExpectedSymbol = expectedSymbol.Length == 0 || SymbolMatches(fill.Symbol, expectedSymbol);

                        if (isExpectedTerminal && isExpectedSymbol)
                        {
                            Log?.Invoke(string.Concat(
                                "[EXECUTION ERROR] Broker rejected order. Code: ",
                                fill.Ticket.ToString(CultureInfo.InvariantCulture)));
                            ResetOpenPositionState();
                        }
                    }

                    FillReceived?.Invoke(fill);
                    continue;
                }

                if (_hasOpenPosition && _requestTimeMs > 0)
                {
                    var expectedTerminal = _activeTargetTerminal;
                    var expectedSymbol = _activeSlaveSymbol;
                    var fillTerminal = fill.TerminalId ?? string.Empty;
                    var isExpectedTerminal = expectedTerminal.Length == 0
                                             || string.Equals(fillTerminal, expectedTerminal, StringComparison.OrdinalIgnoreCase);
                    var isExpectedSymbol = expectedSymbol.Length == 0 || SymbolMatches(fill.Symbol, expectedSymbol);

                    if (isExpectedTerminal && isExpectedSymbol)
                    {
                        _isAwaitingFill = false;
                        var fillReceivedMs = (long)(System.Diagnostics.Stopwatch.GetTimestamp() * 1000.0 / System.Diagnostics.Stopwatch.Frequency);
                        var latencyOriginMs = _lastOrderSentTimeMs > 0 ? _lastOrderSentTimeMs : _requestTimeMs;
                        var executionLatency = fillReceivedMs - latencyOriginMs;
                        if (executionLatency < 0) executionLatency = 0;
                        var isBuy = _openSide == 1 || string.Equals(fill.Side, "BUY", StringComparison.OrdinalIgnoreCase);

                        if (fill.Price > 0)
                        {
                            var slippage = isBuy ? (fill.Price - _requestPrice) : (_requestPrice - fill.Price);
                            var absSlippage = Math.Abs(slippage);
                            _openPrice = fill.Price;
                            _openTicket = fill.Ticket;
                            _positionOpenTimeMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
                            if (_execQualitySamples == 0)
                            {
                                _slippageAbsEwma = absSlippage;
                                _latencyMsEwma = executionLatency;
                            }
                            else
                            {
                                _slippageAbsEwma = (_slippageAbsEwma * 0.8) + (absSlippage * 0.2);
                                _latencyMsEwma = (_latencyMsEwma * 0.8) + (executionLatency * 0.2);
                            }
                            _execQualitySamples++;

                            // Re-anchor targets to actual fill to keep risk/reward consistent after slippage.
                            if (_openSide == 1)
                            {
                                _targetTpPrice = fill.Price + _entryGapDistance;
                                _targetSlPrice = _entryStopDistance > 0.0 ? (fill.Price - _entryStopDistance) : 0.0;
                            }
                            else if (_openSide == -1)
                            {
                                _targetTpPrice = fill.Price - _entryGapDistance;
                                _targetSlPrice = _entryStopDistance > 0.0 ? (fill.Price + _entryStopDistance) : 0.0;
                            }

                            Log?.Invoke(string.Concat(
                                "[EXECUTION] ",
                                fill.Side ?? string.Empty,
                                " | ReqPrice: ", _requestPrice.ToString("F5", CultureInfo.InvariantCulture),
                                " | FillPrice: ", fill.Price.ToString("F5", CultureInfo.InvariantCulture),
                                " | Slippage: ", slippage.ToString("F5", CultureInfo.InvariantCulture),
                                " | Latency: ", executionLatency.ToString(CultureInfo.InvariantCulture), "ms"));
                            Log?.Invoke(string.Concat(
                                "[ENGINE] Re-anchored targets to fill. TP=",
                                _targetTpPrice.ToString("F5", CultureInfo.InvariantCulture),
                                " SL=",
                                _targetSlPrice.ToString("F5", CultureInfo.InvariantCulture)));
                        }
                        else
                        {
                            Log?.Invoke(string.Concat(
                                "[EXECUTION] ",
                                fill.Side ?? string.Empty,
                                " | ReqPrice: ", _requestPrice.ToString("F5", CultureInfo.InvariantCulture),
                                " | FillPrice: N/A | Latency: ", executionLatency.ToString(CultureInfo.InvariantCulture), "ms"));
                        }

                        if (executionLatency > _maxExecutionLatencyMs)
                        {
                            var killTerminal = expectedTerminal.Length > 0 ? expectedTerminal : fillTerminal;
                            var killSymbol = expectedSymbol.Length > 0 ? expectedSymbol : fill.Symbol;
                            _pipes.PublishCommand(BuildKillCommand(killTerminal, killSymbol));
                            Log?.Invoke(string.Concat(
                                "[TOXIC FILL] Round-trip execution latency of ",
                                executionLatency.ToString(CultureInfo.InvariantCulture),
                                "ms exceeded the limit of ",
                                _maxExecutionLatencyMs.ToString(CultureInfo.InvariantCulture),
                                "ms. Sent immediate KILL."));
                        }
                        else
                        {
                            var tpReached = false;
                            var slExceeded = false;
                            if (fill.Price > 0 && _targetTpPrice > 0)
                            {
                                tpReached = isBuy ? (fill.Price >= _targetTpPrice) : (fill.Price <= _targetTpPrice);
                            }

                            if (fill.Price > 0 && _targetSlPrice > 0)
                            {
                                slExceeded = isBuy ? (fill.Price <= _targetSlPrice) : (fill.Price >= _targetSlPrice);
                            }

                            if (tpReached || slExceeded)
                            {
                                var killTerminal = expectedTerminal.Length > 0 ? expectedTerminal : fillTerminal;
                                var killSymbol = expectedSymbol.Length > 0 ? expectedSymbol : fill.Symbol;
                                _pipes.PublishCommand(BuildKillCommand(killTerminal, killSymbol));
                                Log?.Invoke(string.Concat(
                                    "[ENGINE] Fill already crossed target bounds (TPReached=",
                                    tpReached ? "true" : "false",
                                    ", SLExceeded=",
                                    slExceeded ? "true" : "false",
                                    "). Sent immediate KILL."));
                            }
                            else
                            {
                                var modifyTerminal = expectedTerminal.Length > 0 ? expectedTerminal : fillTerminal;
                                var modifySymbol = expectedSymbol.Length > 0 ? expectedSymbol : fill.Symbol;
                                var modifyTicket = fill.Ticket > 0 ? fill.Ticket : _openTicket;
                                _pipes.PublishCommand(BuildModifyCommand(modifyTerminal, modifySymbol, modifyTicket, _targetSlPrice, _targetTpPrice));
                                Log?.Invoke(string.Concat(
                                    "[ENGINE] Sent MODIFY for absolute targets. TP=",
                                    _targetTpPrice.ToString("F5", CultureInfo.InvariantCulture),
                                    ", SL=",
                                    _targetSlPrice.ToString("F5", CultureInfo.InvariantCulture),
                                    ", Ticket=",
                                    modifyTicket.ToString(CultureInfo.InvariantCulture)));
                            }
                        }

                        _requestTimeMs = 0;
                        _lastOrderSentTimeMs = 0;
                        _isAwaitingFill = false;
                    }
                }

                FillReceived?.Invoke(fill);
            }
        }
    }

    private void OnPositionClosed(string terminalId)
    {
        var targetTerminal = _activeTargetTerminal;
        if (targetTerminal.Length == 0)
        {
            lock (_configLock)
            {
                targetTerminal = _targetTerminal.Trim();
            }
        }
        if (targetTerminal.Length == 0) return;
        if (!string.Equals((terminalId ?? string.Empty).Trim(), targetTerminal, StringComparison.OrdinalIgnoreCase)) return;
        _lastFlatTimeMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        ResetOpenPositionState();
    }

    private static string BuildKillCommand(string terminal, string symbol)
    {
        return string.Concat(_cmdKillPrefix, terminal, "|", symbol);
    }

    private static string BuildTradeCommand(string side, string symbol, double lot, double sl, double tp, string terminal, long timestampMs)
    {
        return string.Concat(
            _cmdTradePrefix, terminal,
            "|", symbol,
            "|", side,
            "|", lot.ToString("F2", CultureInfo.InvariantCulture),
            "|", sl.ToString("0.#####", CultureInfo.InvariantCulture),
            "|", tp.ToString("0.#####", CultureInfo.InvariantCulture),
            "|", timestampMs.ToString(CultureInfo.InvariantCulture));
    }

    private static string BuildModifyCommand(string terminal, string symbol, long ticket, double sl, double tp)
    {
        return string.Concat(
            _cmdModifyPrefix, terminal,
            "|", symbol,
            "|", ticket.ToString(CultureInfo.InvariantCulture),
            "|", sl.ToString("0.#####", CultureInfo.InvariantCulture),
            "|", tp.ToString("0.#####", CultureInfo.InvariantCulture));
    }

    private void ResetEntryPersistence()
    {
        // Persistence filter removed: entry is now driven only by trigger threshold and max spread.
    }

    private void ResetOpenPositionState()
    {
        _hasOpenPosition = false;
        _isAwaitingFill = false;
        _positionOpenTimeMs = 0;
        _openPrice = 0.0;
        _openSide = 0;
        _requestTimeMs = 0;
        _lastOrderSentTimeMs = 0;
        _requestPrice = 0.0;
        _entryGapDistance = 0.0;
        _entryStopDistance = 0.0;
        _targetTpPrice = 0.0;
        _targetSlPrice = 0.0;
        _openTicket = 0;
        _activeTargetTerminal = string.Empty;
        _activeSlaveSymbol = string.Empty;
        ResetEntryPersistence();
    }

    private static bool SymbolMatches(string receivedSymbol, string configuredSymbol)
    {
        var received = NormalizeSymbol(receivedSymbol);
        var configured = NormalizeSymbol(configuredSymbol);

        if (configured.Length == 0)
        {
            return true;
        }

        if (received.Length == 0)
        {
            return false;
        }

        if (string.Equals(received, configured, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return received.StartsWith(configured, StringComparison.OrdinalIgnoreCase)
               || configured.StartsWith(received, StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeSymbol(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return string.Empty;
        }

        var input = symbol.Trim().ToUpperInvariant();
        var sb = new StringBuilder(input.Length);
        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
            {
                sb.Append(c);
            }
        }

        return sb.Length > 0 ? sb.ToString() : input;
    }
}

public sealed class EngineConfig
{
    public double LotSize { get; set; }
    public double MaxSpread { get; set; }
    public double TriggerThreshold { get; set; }
    public double HardStopLoss { get; set; }
    public bool UseRithmic { get; set; }
    public string? RithmicUsername { get; set; }
    public string? RithmicPassword { get; set; }
    public string? RithmicSystem { get; set; }
    public string? RithmicGateway { get; set; }
    public string? SourceTerminal { get; set; }
    public string? TargetTerminal { get; set; }
    public string? MasterSymbol { get; set; }
    public string? SlaveSymbol { get; set; }
}

public readonly struct AnalysisSample
{
    public double Gap { get; }
    public double Deviation { get; }
    public double LatencyMs { get; }
    public double Spread { get; }

    public AnalysisSample(double gap, double deviation, double latencyMs, double spread)
    {
        Gap = gap;
        Deviation = deviation;
        LatencyMs = latencyMs;
        Spread = spread;
    }
}

public readonly struct AnalysisResult
{
    public double AverageGap { get; }
    public double MaxGap { get; }
    public double StaticOffset { get; }
    public double PeakSpike { get; }
    public double AverageLatencyMs { get; }
    public double AverageSpread { get; }
    public double WinRate { get; }
    public int SampleCount { get; }
    public string Status { get; }

    public AnalysisResult(double avgGap, double maxGap, double staticOffset, double peakSpike, double avgLatencyMs, double avgSpread, double winRate, int sampleCount, string status)
    {
        AverageGap = avgGap;
        MaxGap = maxGap;
        StaticOffset = staticOffset;
        PeakSpike = peakSpike;
        AverageLatencyMs = avgLatencyMs;
        AverageSpread = avgSpread;
        WinRate = winRate;
        SampleCount = sampleCount;
        Status = status;
    }

    public static AnalysisResult Empty(string status)
    {
        return new AnalysisResult(0, 0, 0, 0, 0, 0, 0, 0, status);
    }
}

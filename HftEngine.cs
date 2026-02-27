using System;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;

namespace ArbitrageX;

public readonly struct HftExecutionReport
{
    public long Seq { get; }
    public int Status { get; }
    public int Side { get; }
    public long Ticket { get; }
    public double FillPrice { get; }
    public double FillLots { get; }
    public long Timestamp { get; }
    public int ErrorCode { get; }

    public HftExecutionReport(long seq, int status, int side, long ticket, double fillPrice, double fillLots, long timestamp, int errorCode)
    {
        Seq = seq;
        Status = status;
        Side = side;
        Ticket = ticket;
        FillPrice = fillPrice;
        FillLots = fillLots;
        Timestamp = timestamp;
        ErrorCode = errorCode;
    }
}

public sealed class HftEngine : IDisposable
{
    private enum EngineState
    {
        Flat = 0,
        WaitingFill = 1,
        InPosition = 2
    }

    private const int DefaultMaxHoldTimeMs = 3000;
    private const int Mt5RetcodeDone = 10009;
    private const long CooldownUs = 1_000_000;

    private readonly object _lifecycleLock = new();
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;

    private Thread? _thread;
    private CancellationTokenSource? _cts;
    private int _disposed;

    private long _lastMasterSeq;
    private long _lastSlaveSeq;
    private long _lastReportSeq;
    private long _signalSeq;

    private double _masterBid;
    private double _masterAsk;
    private double _slaveBid;
    private double _slaveAsk;
    private bool _hasMaster;
    private bool _hasSlave;

    private double _emaOffset;
    private bool _emaInitialized;
    private bool _emaFrozen;
    private double _rawOffset;

    private double _buyEdge;
    private double _sellEdge;
    private double _currentGap;

    private bool _armedBuy;
    private bool _armedSell;

    private EngineState _state;
    private int _pendingSide;
    private int _positionSide;
    private long _positionTicket;
    private long _entrySignalUs;
    private long _positionOpenUs;
    private long _cooldownUntilUs;
    private bool _exitRequested;

    private double _threshold = 1.5;
    private double _lotSize = 0.1;
    private double _calibrationAlpha = 0.001;
    private double _maxDeviationPoints = 2.0;
    private int _maxHoldTimeMs = DefaultMaxHoldTimeMs;

    private long _ticksThisSecond;
    private long _tpsClockTick;
    private long _currentTps;

    private volatile bool _tradingEnabled;
    private volatile bool _isEngineCalibrating;

    public long CurrentTps => Volatile.Read(ref _currentTps);
    public double CurrentGap => Interlocked.CompareExchange(ref _currentGap, 0.0, 0.0);
    public bool IsEngineCalibrating => _isEngineCalibrating;
    public bool TradingEnabled => _tradingEnabled;

    public event Action<string>? Log;
    public event Action<HftExecutionReport>? ExecutionReportReceived;

    public HftEngine()
    {
        _mmf = MemoryMappedFile.CreateOrOpen(
            NexusMmfLayout.MapName,
            NexusMmfLayout.MapSize,
            MemoryMappedFileAccess.ReadWrite);
        _view = _mmf.CreateViewAccessor(0, NexusMmfLayout.MapSize, MemoryMappedFileAccess.ReadWrite);
    }

    public void UpdateConfig(EngineConfig config)
    {
        Interlocked.Exchange(ref _lotSize, config.LotSize > 0.0 ? config.LotSize : 0.1);
        Interlocked.Exchange(ref _threshold, config.TriggerThreshold > 0.0 ? config.TriggerThreshold : 1.5);

        var alpha = config.CalibrationAlpha > 0.0 ? config.CalibrationAlpha : 0.001;
        if (alpha > 1.0) alpha = 1.0;
        Interlocked.Exchange(ref _calibrationAlpha, alpha);

        var holdMs = config.MaxHoldTimeMs > 0 ? config.MaxHoldTimeMs : DefaultMaxHoldTimeMs;
        Volatile.Write(ref _maxHoldTimeMs, holdMs);

        var deviation = config.MaxDeviationPoints;
        if (double.IsNaN(deviation) || double.IsInfinity(deviation) || deviation < 0.0)
        {
            deviation = 0.0;
        }
        else if (deviation > 50.0)
        {
            deviation = 50.0;
        }
        Interlocked.Exchange(ref _maxDeviationPoints, deviation);
    }

    public void Start()
    {
        lock (_lifecycleLock)
        {
            if (_thread is { IsAlive: true })
            {
                return;
            }

            _cts?.Cancel();
            _cts?.Dispose();
            _cts = new CancellationTokenSource();

            ResetForStart();

            _thread = new Thread(() => Loop(_cts.Token))
            {
                IsBackground = true,
                Name = "HftEngine",
                Priority = ThreadPriority.Highest
            };
            _thread.Start();
        }
    }

    public void Stop()
    {
        CancellationTokenSource? cts;
        Thread? thread;
        lock (_lifecycleLock)
        {
            _tradingEnabled = false;
            cts = _cts;
            thread = _thread;
            _cts = null;
            _thread = null;
        }

        if (cts is null)
        {
            return;
        }

        try
        {
            cts.Cancel();
        }
        catch
        {
            // ignored
        }

        try
        {
            thread?.Join(250);
        }
        catch
        {
            // ignored
        }

        cts.Dispose();
        _isEngineCalibrating = false;
        _currentGap = 0.0;
        _currentTps = 0;
    }

    public void KillSwitch()
    {
        if (!_tradingEnabled)
        {
            return;
        }

        WriteSignal(NexusMmfLayout.SignalCommandKill, 0, 0.0, _rawOffset - _emaOffset);
        _tradingEnabled = false;
        _exitRequested = true;
        _cooldownUntilUs = NowMonoUs() + CooldownUs;
        Log?.Invoke("HFT_KILL_SWITCH");
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Stop();
        _view.Dispose();
        _mmf.Dispose();
    }

    private void ResetForStart()
    {
        _lastMasterSeq = _view.ReadInt64(NexusMmfLayout.MasterTickOffset + NexusMmfLayout.TickSeqOffset);
        _lastSlaveSeq = _view.ReadInt64(NexusMmfLayout.SlaveTickOffset + NexusMmfLayout.TickSeqOffset);
        _lastReportSeq = _view.ReadInt64(NexusMmfLayout.ExecutionReportOffset + NexusMmfLayout.ReportSeqOffset);
        _signalSeq = _view.ReadInt64(NexusMmfLayout.TradeSignalOffset + NexusMmfLayout.SignalSeqOffset);

        _masterBid = 0.0;
        _masterAsk = 0.0;
        _slaveBid = 0.0;
        _slaveAsk = 0.0;
        _hasMaster = false;
        _hasSlave = false;

        _emaOffset = 0.0;
        _emaInitialized = false;
        _emaFrozen = false;
        _rawOffset = 0.0;
        _isEngineCalibrating = true;

        _buyEdge = 0.0;
        _sellEdge = 0.0;
        _currentGap = 0.0;

        _armedBuy = true;
        _armedSell = true;

        _state = EngineState.Flat;
        _pendingSide = 0;
        _positionSide = 0;
        _positionTicket = 0;
        _entrySignalUs = 0;
        _positionOpenUs = 0;
        _cooldownUntilUs = 0;
        _exitRequested = false;

        _tradingEnabled = true;
        _tpsClockTick = Stopwatch.GetTimestamp();
        _ticksThisSecond = 0;
        _currentTps = 0;
    }

    private void Loop(CancellationToken token)
    {
        var spin = new SpinWait();

        while (!token.IsCancellationRequested)
        {
            var any = false;
            any |= TryReadMasterTick();
            any |= TryReadSlaveTick();
            any |= TryReadExecutionReport();

            if (_hasMaster && _hasSlave)
            {
                RecomputeModel();
                UpdateArming();

                var nowUs = NowMonoUs();
                if (_state == EngineState.Flat)
                {
                    if (_tradingEnabled)
                    {
                        TryOpen(nowUs);
                    }
                }
                else if (_state == EngineState.InPosition)
                {
                    CheckExit(nowUs);
                }
            }

            UpdateTpsClock();

            if (any)
            {
                spin.Reset();
            }
            else
            {
                spin.SpinOnce();
            }
        }
    }

    private bool TryReadMasterTick()
    {
        if (!TryReadTick(NexusMmfLayout.MasterTickOffset, ref _lastMasterSeq, out var bid, out var ask))
        {
            return false;
        }

        _masterBid = bid;
        _masterAsk = ask;
        _hasMaster = bid > 0.0 && ask > 0.0;
        _ticksThisSecond++;
        return true;
    }

    private bool TryReadSlaveTick()
    {
        if (!TryReadTick(NexusMmfLayout.SlaveTickOffset, ref _lastSlaveSeq, out var bid, out var ask))
        {
            return false;
        }

        _slaveBid = bid;
        _slaveAsk = ask;
        _hasSlave = bid > 0.0 && ask > 0.0;
        _ticksThisSecond++;
        return true;
    }

    private bool TryReadTick(long offset, ref long lastSeq, out double bid, out double ask)
    {
        bid = 0.0;
        ask = 0.0;

        var seq1 = _view.ReadInt64(offset + NexusMmfLayout.TickSeqOffset);
        if (seq1 <= 0 || seq1 == lastSeq)
        {
            return false;
        }

        var localBid = _view.ReadDouble(offset + NexusMmfLayout.TickBidOffset);
        var localAsk = _view.ReadDouble(offset + NexusMmfLayout.TickAskOffset);
        var seq2 = _view.ReadInt64(offset + NexusMmfLayout.TickSeqOffset);
        if (seq1 != seq2)
        {
            return false;
        }

        lastSeq = seq2;
        bid = localBid;
        ask = localAsk;
        return true;
    }

    private bool TryReadExecutionReport()
    {
        var reportOffset = NexusMmfLayout.ExecutionReportOffset;
        var seq1 = _view.ReadInt64(reportOffset + NexusMmfLayout.ReportSeqOffset);
        if (seq1 <= 0 || seq1 == _lastReportSeq)
        {
            return false;
        }

        var status = _view.ReadInt32(reportOffset + NexusMmfLayout.ReportStatusOffset);
        var side = _view.ReadInt32(reportOffset + NexusMmfLayout.ReportSideOffset);
        var ticket = _view.ReadInt64(reportOffset + NexusMmfLayout.ReportTicketOffset);
        var fillPrice = _view.ReadDouble(reportOffset + NexusMmfLayout.ReportFillPriceOffset);
        var fillLots = _view.ReadDouble(reportOffset + NexusMmfLayout.ReportFillLotsOffset);
        var timestamp = _view.ReadInt64(reportOffset + NexusMmfLayout.ReportTimestampOffset);
        var errorCode = _view.ReadInt32(reportOffset + NexusMmfLayout.ReportErrorCodeOffset);
        var seq2 = _view.ReadInt64(reportOffset + NexusMmfLayout.ReportSeqOffset);

        if (seq1 != seq2)
        {
            return false;
        }

        _lastReportSeq = seq2;
        ApplyReport(status, side, ticket, errorCode);
        ExecutionReportReceived?.Invoke(new HftExecutionReport(seq2, status, side, ticket, fillPrice, fillLots, timestamp, errorCode));
        return true;
    }

    private void ApplyReport(int status, int side, long ticket, int errorCode)
    {
        if (errorCode == Mt5RetcodeDone && status <= NexusMmfLayout.ReportStatusRejected)
        {
            status = NexusMmfLayout.ReportStatusFilled;
        }

        if (status == NexusMmfLayout.ReportStatusAccepted)
        {
            if (_state == EngineState.InPosition || _exitRequested)
            {
                if (ticket > 0)
                {
                    _positionTicket = ticket;
                }
                return;
            }

            _state = EngineState.WaitingFill;
            if (side != 0)
            {
                _pendingSide = side;
            }
            if (ticket > 0)
            {
                _positionTicket = ticket;
            }
            return;
        }

        if (status == NexusMmfLayout.ReportStatusFilled)
        {
            _state = EngineState.InPosition;
            _positionSide = side != 0 ? side : (_pendingSide != 0 ? _pendingSide : _positionSide);
            if (_positionSide == 0)
            {
                _positionSide = 1;
            }

            if (ticket > 0)
            {
                _positionTicket = ticket;
            }

            _entrySignalUs = 0;
            _pendingSide = 0;
            _positionOpenUs = NowMonoUs();
            _exitRequested = false;
            return;
        }

        if (status == NexusMmfLayout.ReportStatusRejected)
        {
            var wasWaitingFill = _state == EngineState.WaitingFill;
            _entrySignalUs = 0;
            _cooldownUntilUs = NowMonoUs() + CooldownUs;
            if (_state == EngineState.InPosition || _exitRequested)
            {
                _state = EngineState.InPosition;
                _pendingSide = 0;
                _exitRequested = false;
            }
            else
            {
                _state = EngineState.Flat;
                _pendingSide = 0;
                _positionSide = 0;
                _positionTicket = 0;
                _positionOpenUs = 0;
                _exitRequested = false;
            }

            if (wasWaitingFill)
            {
                Log?.Invoke("[HFT] Entry REJECTED (IOC Canceled) - Cooldown activated.");
            }
            return;
        }

        if (status == NexusMmfLayout.ReportStatusFlat || status < 0)
        {
            var wasPositionState = _state == EngineState.InPosition || _exitRequested;
            var closedSide = _positionSide != 0 ? _positionSide : (_pendingSide != 0 ? _pendingSide : side);
            if (closedSide > 0)
            {
                _armedBuy = false;
            }
            else if (closedSide < 0)
            {
                _armedSell = false;
            }

            _state = EngineState.Flat;
            _pendingSide = 0;
            _positionSide = 0;
            _positionTicket = 0;
            _entrySignalUs = 0;
            _positionOpenUs = 0;
            if (wasPositionState)
            {
                _cooldownUntilUs = NowMonoUs() + CooldownUs;
            }
            _exitRequested = false;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long NowMonoUs()
    {
        return (long)(Stopwatch.GetTimestamp() * 1000000.0 / Stopwatch.Frequency);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double FastAbs(double value)
    {
        return value >= 0.0 ? value : -value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecomputeModel()
    {
        var masterMid = (_masterBid + _masterAsk) * 0.5;
        var slaveMid = (_slaveBid + _slaveAsk) * 0.5;
        _rawOffset = masterMid - slaveMid;

        if (!_emaInitialized)
        {
            _emaOffset = _rawOffset;
            _emaInitialized = true;
            _emaFrozen = false;
            _isEngineCalibrating = false;
        }
        else
        {
            var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
            var freezeBand = threshold > 0.0 ? (threshold * 0.5) : 0.0;
            var diff = _rawOffset - _emaOffset;
            var shouldFreeze = FastAbs(diff) >= freezeBand;
            if (!shouldFreeze)
            {
                var alpha = Interlocked.CompareExchange(ref _calibrationAlpha, 0.0, 0.0);
                _emaOffset = (_emaOffset * (1.0 - alpha)) + (_rawOffset * alpha);
                _emaFrozen = false;
            }
            else
            {
                _emaFrozen = true;
            }
        }

        var expectedSlaveBid = _masterBid - _emaOffset;
        var expectedSlaveAsk = _masterAsk - _emaOffset;

        _buyEdge = expectedSlaveBid - _slaveAsk;
        _sellEdge = _slaveBid - expectedSlaveAsk;

        Interlocked.Exchange(ref _currentGap, _rawOffset - _emaOffset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateArming()
    {
        if (!_armedBuy && _buyEdge <= 0.0)
        {
            _armedBuy = true;
        }

        if (!_armedSell && _sellEdge <= 0.0)
        {
            _armedSell = true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void TryOpen(long nowUs)
    {
        if (nowUs < _cooldownUntilUs)
        {
            return;
        }

        var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
        var spread = _slaveAsk - _slaveBid;
        if (spread < 0.0)
        {
            spread = 0.0;
        }

        if (spread > (threshold * 2.0))
        {
            return;
        }

        var canBuy = _armedBuy && _buyEdge >= threshold;
        var canSell = _armedSell && _sellEdge >= threshold;
        if (!canBuy && !canSell)
        {
            return;
        }

        if (canBuy && (!canSell || _buyEdge >= _sellEdge))
        {
            WriteSignal(NexusMmfLayout.SignalCommandOpen, 1, _slaveAsk, _buyEdge);
            _state = EngineState.WaitingFill;
            _pendingSide = 1;
            _entrySignalUs = nowUs;
            _exitRequested = false;
            LogEdge("HFT_ENTRY_BUY");
            return;
        }

        WriteSignal(NexusMmfLayout.SignalCommandOpen, -1, _slaveBid, _sellEdge);
        _state = EngineState.WaitingFill;
        _pendingSide = -1;
        _entrySignalUs = nowUs;
        _exitRequested = false;
        LogEdge("HFT_ENTRY_SELL");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckExit(long nowUs)
    {
        if (_exitRequested || _positionSide == 0)
        {
            return;
        }

        if (nowUs < _cooldownUntilUs)
        {
            return;
        }

        var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
        var hysteresis = threshold * 0.2;

        if (_positionSide > 0)
        {
            if (_rawOffset <= _emaOffset - hysteresis)
            {
                SendKill("HFT_EXIT_BUY_RAW", nowUs);
                return;
            }
        }
        else
        {
            if (_rawOffset >= _emaOffset + hysteresis)
            {
                SendKill("HFT_EXIT_SELL_RAW", nowUs);
                return;
            }
        }

        var holdMs = Volatile.Read(ref _maxHoldTimeMs);
        if (holdMs <= 0)
        {
            holdMs = DefaultMaxHoldTimeMs;
        }

        if (_positionOpenUs > 0 && nowUs - _positionOpenUs >= (holdMs * 1000L))
        {
            SendKill("HFT_EXIT_EMERGENCY_TIMEOUT", nowUs);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SendKill(string logTag, long nowUs)
    {
        WriteSignal(NexusMmfLayout.SignalCommandKill, 0, 0.0, _rawOffset - _emaOffset);
        _exitRequested = true;
        _cooldownUntilUs = nowUs + CooldownUs;
        LogEdge(logTag);
    }

    private void WriteSignal(int command, int side, double price, double reference)
    {
        var signalOffset = NexusMmfLayout.TradeSignalOffset;
        var lots = Interlocked.CompareExchange(ref _lotSize, 0.0, 0.0);
        var deviation = Interlocked.CompareExchange(ref _maxDeviationPoints, 0.0, 0.0);

        _view.Write(signalOffset + NexusMmfLayout.SignalCommandOffset, command);
        _view.Write(signalOffset + NexusMmfLayout.SignalSideOffset, side);
        _view.Write(signalOffset + NexusMmfLayout.SignalLotsOffset, lots);
        _view.Write(signalOffset + NexusMmfLayout.SignalPriceOffset, price);
        _view.Write(signalOffset + NexusMmfLayout.SignalMaxDeviationOffset, deviation);
        _view.Write(signalOffset + NexusMmfLayout.SignalReferenceOffset, reference);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveBidOffset, _slaveBid);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveAskOffset, _slaveAsk);

        var nextSeq = Interlocked.Increment(ref _signalSeq);
        _view.Write(signalOffset + NexusMmfLayout.SignalSeqOffset, nextSeq);
    }

    private void LogEdge(string tag)
    {
        Log?.Invoke(string.Concat(
            tag,
            " buyEdge=",
            _buyEdge.ToString("F4"),
            " sellEdge=",
            _sellEdge.ToString("F4"),
            " rawOffset=",
            _rawOffset.ToString("F4"),
            " emaOffset=",
            _emaOffset.ToString("F4"),
            " frozen=",
            _emaFrozen ? "1" : "0"));
    }

    private void UpdateTpsClock()
    {
        var now = Stopwatch.GetTimestamp();
        var elapsedTicks = now - _tpsClockTick;
        if (elapsedTicks < Stopwatch.Frequency)
        {
            return;
        }

        var ticksThisSecond = _ticksThisSecond;
        _ticksThisSecond = 0;
        _tpsClockTick = now;
        if (elapsedTicks <= 0)
        {
            Volatile.Write(ref _currentTps, 0);
            return;
        }

        var tps = (long)(ticksThisSecond * (double)Stopwatch.Frequency / elapsedTicks);
        Volatile.Write(ref _currentTps, tps);
    }
}

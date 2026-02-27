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

    private const int DefaultMaxHoldTimeMs = 1500;
    private const int MinWaitFillMs = 250;
    private const int Mt5RetcodeDone = 10009;

    private const string LogSignalBuy = "HFT_SIGNAL_BUY";
    private const string LogSignalSell = "HFT_SIGNAL_SELL";
    private const string LogExitStop = "HFT_EXIT_VSL";
    private const string LogExitTake = "HFT_EXIT_VTP";
    private const string LogExitTime = "HFT_EXIT_TIMEOUT";
    private const string LogFlat = "HFT_FLAT";
    private const string LogKill = "HFT_KILL";

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
    private double _calibratedEdge;

    private bool _armedBuy;
    private bool _armedSell;

    private EngineState _state;
    private int _pendingSide;
    private int _positionSide;
    private long _positionTicket;
    private double _entryPrice;
    private long _entrySignalMs;
    private long _entryOpenMs;
    private bool _exitRequested;

    private double _threshold = 0.5;
    private double _lotSize = 0.1;
    private double _calibrationAlpha = 0.001;
    private int _maxHoldTimeMs = DefaultMaxHoldTimeMs;
    private double _virtualStopLoss = 5.0;
    private double _virtualTakeProfit = 5.0;

    private long _ticksThisSecond;
    private long _tpsClockTick;
    private long _currentTps;
    private double _currentGap;

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
        Interlocked.Exchange(ref _threshold, config.TriggerThreshold > 0.0 ? config.TriggerThreshold : 0.0);
        Interlocked.Exchange(ref _lotSize, config.LotSize > 0.0 ? config.LotSize : 0.1);

        var alpha = config.CalibrationAlpha > 0.0 ? config.CalibrationAlpha : 0.001;
        if (alpha > 1.0) alpha = 1.0;
        Interlocked.Exchange(ref _calibrationAlpha, alpha);

        var maxHold = config.MaxHoldTimeMs > 0 ? config.MaxHoldTimeMs : DefaultMaxHoldTimeMs;
        Volatile.Write(ref _maxHoldTimeMs, maxHold);

        Interlocked.Exchange(ref _virtualStopLoss, config.VirtualStopLoss > 0.0 ? config.VirtualStopLoss : 0.0);
        Interlocked.Exchange(ref _virtualTakeProfit, config.VirtualTakeProfit > 0.0 ? config.VirtualTakeProfit : 0.0);
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

        WriteSignal(NexusMmfLayout.SignalCommandKill, 0, _calibratedEdge);
        _exitRequested = true;
        _tradingEnabled = false;
        Log?.Invoke(LogKill);
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

        _hasMaster = false;
        _hasSlave = false;
        _masterBid = 0.0;
        _masterAsk = 0.0;
        _slaveBid = 0.0;
        _slaveAsk = 0.0;

        _emaOffset = 0.0;
        _emaInitialized = false;
        _emaFrozen = false;
        _calibratedEdge = 0.0;

        _armedBuy = true;
        _armedSell = true;

        _state = EngineState.Flat;
        _pendingSide = 0;
        _positionSide = 0;
        _positionTicket = 0;
        _entryPrice = 0.0;
        _entrySignalMs = 0;
        _entryOpenMs = 0;
        _exitRequested = false;

        _isEngineCalibrating = true;
        _tradingEnabled = true;
        _tpsClockTick = Stopwatch.GetTimestamp();
        _ticksThisSecond = 0;
        _currentTps = 0;
        _currentGap = 0.0;
    }

    private void Loop(CancellationToken token)
    {
        var spin = new SpinWait();

        while (!token.IsCancellationRequested)
        {
            var anyWork = false;
            anyWork |= TryReadMasterTick();
            anyWork |= TryReadSlaveTick();
            anyWork |= TryReadExecutionReport();

            if (_hasMaster && _hasSlave)
            {
                _calibratedEdge = ComputeCalibratedEdge();
                Interlocked.Exchange(ref _currentGap, _calibratedEdge);
                UpdateArming(_calibratedEdge);

                var nowMs = NowMonoMs();
                if (_state == EngineState.Flat)
                {
                    if (_tradingEnabled)
                    {
                        TryEnter(nowMs, _calibratedEdge);
                    }
                }
                else if (_state == EngineState.WaitingFill)
                {
                    CheckWaitingFillTimeout(nowMs);
                }
                else
                {
                    CheckVirtualExit(nowMs);
                }
            }

            UpdateTpsClock();

            if (anyWork)
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
        ApplyReportState(status, side, ticket, fillPrice, errorCode);
        ExecutionReportReceived?.Invoke(new HftExecutionReport(seq2, status, side, ticket, fillPrice, fillLots, timestamp, errorCode));
        return true;
    }

    private void ApplyReportState(int status, int side, long ticket, double fillPrice, int errorCode)
    {
        if (errorCode == Mt5RetcodeDone &&
            (status == NexusMmfLayout.ReportStatusNone || status == NexusMmfLayout.ReportStatusRejected))
        {
            status = NexusMmfLayout.ReportStatusFilled;
        }

        if (status == NexusMmfLayout.ReportStatusAccepted || status == NexusMmfLayout.ReportStatusFilled)
        {
            _state = EngineState.InPosition;
            _exitRequested = false;

            if (side == 0)
            {
                side = _pendingSide != 0 ? _pendingSide : _positionSide;
            }

            _positionSide = side;
            if (ticket > 0)
            {
                _positionTicket = ticket;
            }

            if (fillPrice > 0.0)
            {
                _entryPrice = fillPrice;
            }
            else if (_entryPrice <= 0.0)
            {
                _entryPrice = side > 0 ? _slaveAsk : _slaveBid;
            }

            _entrySignalMs = 0;
            _pendingSide = 0;
            _entryOpenMs = NowMonoMs();
            return;
        }

        if (status == NexusMmfLayout.ReportStatusRejected)
        {
            _state = EngineState.Flat;
            _pendingSide = 0;
            _entrySignalMs = 0;
            _exitRequested = false;
            return;
        }

        if (status == NexusMmfLayout.ReportStatusFlat || status < 0)
        {
            var closedSide = _positionSide != 0 ? _positionSide : (_pendingSide != 0 ? _pendingSide : side);
            if (closedSide > 0)
            {
                _armedBuy = false;
            }
            else if (closedSide < 0)
            {
                _armedSell = false;
            }

            ResetPositionState();
            _state = EngineState.Flat;
            Log?.Invoke(LogFlat);
        }
    }

    private void ResetPositionState()
    {
        _positionSide = 0;
        _positionTicket = 0;
        _entryPrice = 0.0;
        _entryOpenMs = 0;
        _entrySignalMs = 0;
        _pendingSide = 0;
        _exitRequested = false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long NowMonoMs()
    {
        return (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double FastAbs(double v)
    {
        return v >= 0.0 ? v : -v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ShouldEnterBuy(double edge, double threshold, bool armed)
    {
        return armed && edge > threshold;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ShouldEnterSell(double edge, double threshold, bool armed)
    {
        return armed && edge < -threshold;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ShouldRearmBuy(double edge, bool armed)
    {
        return !armed && edge <= 0.0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ShouldRearmSell(double edge, bool armed)
    {
        return !armed && edge >= 0.0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ShouldFreeze(double absDeviation, double freezeBand)
    {
        return absDeviation > freezeBand;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateArming(double calibratedEdge)
    {
        if (ShouldRearmBuy(calibratedEdge, _armedBuy))
        {
            _armedBuy = true;
        }

        if (ShouldRearmSell(calibratedEdge, _armedSell))
        {
            _armedSell = true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private double ComputeCalibratedEdge()
    {
        var masterMid = (_masterBid + _masterAsk) * 0.5;
        var slaveMid = (_slaveBid + _slaveAsk) * 0.5;
        var rawGap = masterMid - slaveMid;

        if (!_emaInitialized)
        {
            _emaOffset = rawGap;
            _emaInitialized = true;
            _emaFrozen = false;
            _isEngineCalibrating = false;
            return 0.0;
        }

        var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
        var freezeBand = threshold > 0.0 ? (threshold * 0.5) : 0.0;
        var deviation = rawGap - _emaOffset;
        var absDeviation = FastAbs(deviation);

        if (_emaFrozen)
        {
            if (!ShouldFreeze(absDeviation, freezeBand))
            {
                _emaFrozen = false;
                _emaOffset += Interlocked.CompareExchange(ref _calibrationAlpha, 0.0, 0.0) * deviation;
            }
        }
        else
        {
            if (ShouldFreeze(absDeviation, freezeBand))
            {
                _emaFrozen = true;
            }
            else
            {
                _emaOffset += Interlocked.CompareExchange(ref _calibrationAlpha, 0.0, 0.0) * deviation;
            }
        }

        return rawGap - _emaOffset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void TryEnter(long nowMs, double calibratedEdge)
    {
        var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
        if (ShouldEnterBuy(calibratedEdge, threshold, _armedBuy))
        {
            WriteSignal(NexusMmfLayout.SignalCommandOpen, 1, calibratedEdge);
            _state = EngineState.WaitingFill;
            _pendingSide = 1;
            _entrySignalMs = nowMs;
            _entryPrice = _slaveAsk;
            _exitRequested = false;
            Log?.Invoke(LogSignalBuy);
            return;
        }

        if (ShouldEnterSell(calibratedEdge, threshold, _armedSell))
        {
            WriteSignal(NexusMmfLayout.SignalCommandOpen, -1, calibratedEdge);
            _state = EngineState.WaitingFill;
            _pendingSide = -1;
            _entrySignalMs = nowMs;
            _entryPrice = _slaveBid;
            _exitRequested = false;
            Log?.Invoke(LogSignalSell);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckWaitingFillTimeout(long nowMs)
    {
        if (_entrySignalMs <= 0)
        {
            return;
        }

        var timeoutMs = Volatile.Read(ref _maxHoldTimeMs);
        if (timeoutMs < MinWaitFillMs)
        {
            timeoutMs = MinWaitFillMs;
        }

        if (nowMs - _entrySignalMs >= timeoutMs)
        {
            _state = EngineState.Flat;
            _pendingSide = 0;
            _entrySignalMs = 0;
            _exitRequested = false;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckVirtualExit(long nowMs)
    {
        if (_exitRequested || _positionSide == 0)
        {
            return;
        }

        var maxHoldMs = Volatile.Read(ref _maxHoldTimeMs);
        if (maxHoldMs <= 0)
        {
            maxHoldMs = DefaultMaxHoldTimeMs;
        }

        if (_entryOpenMs > 0 && nowMs - _entryOpenMs >= maxHoldMs)
        {
            SendKill(LogExitTime);
            return;
        }

        var stopLoss = Interlocked.CompareExchange(ref _virtualStopLoss, 0.0, 0.0);
        var takeProfit = Interlocked.CompareExchange(ref _virtualTakeProfit, 0.0, 0.0);

        if (_positionSide > 0)
        {
            if (takeProfit > 0.0 && _slaveBid >= _entryPrice + takeProfit)
            {
                SendKill(LogExitTake);
                return;
            }

            if (stopLoss > 0.0 && _slaveBid <= _entryPrice - stopLoss)
            {
                SendKill(LogExitStop);
            }
            return;
        }

        if (takeProfit > 0.0 && _slaveAsk <= _entryPrice - takeProfit)
        {
            SendKill(LogExitTake);
            return;
        }

        if (stopLoss > 0.0 && _slaveAsk >= _entryPrice + stopLoss)
        {
            SendKill(LogExitStop);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SendKill(string logMessage)
    {
        WriteSignal(NexusMmfLayout.SignalCommandKill, 0, _calibratedEdge);
        _exitRequested = true;
        Log?.Invoke(logMessage);
    }

    private void WriteSignal(int command, int side, double referenceValue)
    {
        var signalOffset = NexusMmfLayout.TradeSignalOffset;
        var lots = Interlocked.CompareExchange(ref _lotSize, 0.0, 0.0);

        _view.Write(signalOffset + NexusMmfLayout.SignalCommandOffset, command);
        _view.Write(signalOffset + NexusMmfLayout.SignalSideOffset, side);
        _view.Write(signalOffset + NexusMmfLayout.SignalLotsOffset, lots);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlOffset, 0.0);
        _view.Write(signalOffset + NexusMmfLayout.SignalTpOffset, 0.0);
        _view.Write(signalOffset + NexusMmfLayout.SignalReferenceOffset, referenceValue);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveBidOffset, _slaveBid);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveAskOffset, _slaveAsk);

        var nextSeq = Interlocked.Increment(ref _signalSeq);
        _view.Write(signalOffset + NexusMmfLayout.SignalSeqOffset, nextSeq);
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

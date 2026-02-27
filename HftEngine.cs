using System;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
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
    private const int CalibrationWindowSize = 1000;
    private const int MinCalibrationSamples = 64;

    private enum EngineState
    {
        Flat = 0,
        WaitingFill = 1,
        InPosition = 2
    }

    private readonly object _lifecycleLock = new();
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;

    private Thread? _thread;
    private CancellationTokenSource? _cts;

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

    private EngineState _state;
    private int _positionSide;
    private long _positionTicket;
    private long _lastStateLogSeq;

    private double _threshold = 0.5;
    private double _lotSize = 0.1;
    private double _hardStopLoss = 5.0;

    private readonly double[] _offsetWindow = new double[CalibrationWindowSize];
    private int _offsetIndex;
    private int _offsetCount;
    private double _offsetSum;
    private double _rollingOffset;

    private long _ticksThisSecond;
    private long _tpsClockTick;
    private long _currentTps;
    private double _currentGap;
    private volatile bool _isEngineCalibrating;

    private volatile bool _tradingEnabled;
    private int _disposed;

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
        Interlocked.Exchange(ref _hardStopLoss, config.HardStopLoss > 0.0 ? config.HardStopLoss : 0.0);
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
            _state = EngineState.Flat;
            _positionSide = 0;
            _positionTicket = 0;
            _lastStateLogSeq = 0;
            _offsetIndex = 0;
            _offsetCount = 0;
            _offsetSum = 0.0;
            _rollingOffset = 0.0;
            _isEngineCalibrating = true;
            _tradingEnabled = true;
            _tpsClockTick = Stopwatch.GetTimestamp();
            _ticksThisSecond = 0;
            _currentTps = 0;
            _currentGap = 0.0;

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
    }

    public void KillSwitch()
    {
        if (!_tradingEnabled)
        {
            return;
        }

        WriteSignal(NexusMmfLayout.SignalCommandKill, 0);
        _state = EngineState.Flat;
        _positionSide = 0;
        _positionTicket = 0;
        _tradingEnabled = false;
        EmitStateLog("KILL");
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

    private void Loop(CancellationToken token)
    {
        var spin = new SpinWait();

        while (!token.IsCancellationRequested)
        {
            var anyWork = false;
            anyWork |= TryReadMasterTick();
            anyWork |= TryReadSlaveTick();
            anyWork |= TryReadExecutionReport();

            if (_tradingEnabled && _state == EngineState.Flat && _hasMaster && _hasSlave)
            {
                EvaluateAndSignal();
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

    private void EvaluateAndSignal()
    {
        if (_masterBid <= 0.0 || _masterAsk <= 0.0 || _slaveBid <= 0.0 || _slaveAsk <= 0.0)
        {
            return;
        }

        UpdateRollingOffset();
        _isEngineCalibrating = _offsetCount < MinCalibrationSamples;
        if (_isEngineCalibrating)
        {
            return;
        }

        var threshold = Interlocked.CompareExchange(ref _threshold, 0.0, 0.0);
        var mappedMasterBid = _masterBid - _rollingOffset;
        var mappedMasterAsk = _masterAsk - _rollingOffset;
        var buyEdge = mappedMasterBid - _slaveAsk;
        var sellEdge = _slaveBid - mappedMasterAsk;
        var dominantGap = buyEdge >= sellEdge ? buyEdge : sellEdge;
        Interlocked.Exchange(ref _currentGap, dominantGap);

        if (buyEdge > threshold && buyEdge >= sellEdge)
        {
            ComputeStops(1, out var sl, out var tp);
            WriteSignal(NexusMmfLayout.SignalCommandOpen, 1, sl, tp);
            _state = EngineState.WaitingFill;
            EmitSignalLog("BUY", buyEdge, threshold);
            return;
        }

        if (sellEdge > threshold)
        {
            ComputeStops(-1, out var sl, out var tp);
            WriteSignal(NexusMmfLayout.SignalCommandOpen, -1, sl, tp);
            _state = EngineState.WaitingFill;
            EmitSignalLog("SELL", sellEdge, threshold);
        }
    }

    private void WriteSignal(int command, int side, double sl = 0.0, double tp = 0.0)
    {
        var signalOffset = NexusMmfLayout.TradeSignalOffset;
        var lots = Interlocked.CompareExchange(ref _lotSize, 0.0, 0.0);

        _view.Write(signalOffset + NexusMmfLayout.SignalCommandOffset, command);
        _view.Write(signalOffset + NexusMmfLayout.SignalSideOffset, side);
        _view.Write(signalOffset + NexusMmfLayout.SignalLotsOffset, lots);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlOffset, sl);
        _view.Write(signalOffset + NexusMmfLayout.SignalTpOffset, tp);
        _view.Write(signalOffset + NexusMmfLayout.SignalReferenceOffset, _rollingOffset);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveBidOffset, _slaveBid);
        _view.Write(signalOffset + NexusMmfLayout.SignalSlaveAskOffset, _slaveAsk);

        var nextSeq = Interlocked.Increment(ref _signalSeq);
        _view.Write(signalOffset + NexusMmfLayout.SignalSeqOffset, nextSeq);
    }

    private void UpdateRollingOffset()
    {
        var masterMid = (_masterBid + _masterAsk) * 0.5;
        var slaveMid = (_slaveBid + _slaveAsk) * 0.5;
        var diff = masterMid - slaveMid;

        if (_offsetCount < CalibrationWindowSize)
        {
            _offsetWindow[_offsetCount] = diff;
            _offsetSum += diff;
            _offsetCount++;
            if (_offsetCount == CalibrationWindowSize)
            {
                _offsetIndex = 0;
            }
        }
        else
        {
            _offsetSum -= _offsetWindow[_offsetIndex];
            _offsetWindow[_offsetIndex] = diff;
            _offsetSum += diff;
            _offsetIndex++;
            if (_offsetIndex >= CalibrationWindowSize)
            {
                _offsetIndex = 0;
            }
        }

        if (_offsetCount > 0)
        {
            _rollingOffset = _offsetSum / _offsetCount;
        }
    }

    private void ComputeStops(int side, out double sl, out double tp)
    {
        sl = 0.0;
        tp = 0.0;

        var hardStopLoss = Interlocked.CompareExchange(ref _hardStopLoss, 0.0, 0.0);
        if (hardStopLoss <= 0.0)
        {
            return;
        }

        if (side > 0)
        {
            var entry = _slaveAsk;
            if (entry <= 0.0)
            {
                return;
            }

            sl = entry - hardStopLoss;
            tp = entry + hardStopLoss;
            return;
        }

        var sellEntry = _slaveBid;
        if (sellEntry <= 0.0)
        {
            return;
        }

        sl = sellEntry + hardStopLoss;
        tp = sellEntry - hardStopLoss;
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
        ApplyReportState(status, side, ticket);

        var report = new HftExecutionReport(seq2, status, side, ticket, fillPrice, fillLots, timestamp, errorCode);
        ExecutionReportReceived?.Invoke(report);
        EmitExecutionLog(report);
        return true;
    }

    private void ApplyReportState(int status, int side, long ticket)
    {
        if (status == NexusMmfLayout.ReportStatusAccepted || status == NexusMmfLayout.ReportStatusFilled)
        {
            _state = EngineState.InPosition;
            if (side != 0)
            {
                _positionSide = side;
            }

            if (ticket > 0)
            {
                _positionTicket = ticket;
            }

            EmitStateLog("IN_POSITION");
            return;
        }

        if (status == NexusMmfLayout.ReportStatusRejected || status == NexusMmfLayout.ReportStatusFlat || status < 0)
        {
            _state = EngineState.Flat;
            _positionSide = 0;
            _positionTicket = 0;
            EmitStateLog("FLAT");
        }
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

    private void EmitSignalLog(string sideText, double edge, double threshold)
    {
        Log?.Invoke(string.Concat(
            "[HFT] SIGNAL ",
            sideText,
            " calibratedEdge=",
            edge.ToString("F5", CultureInfo.InvariantCulture),
            " threshold=",
            threshold.ToString("F5", CultureInfo.InvariantCulture),
            " offset=",
            _rollingOffset.ToString("F5", CultureInfo.InvariantCulture),
            " SBid=",
            _slaveBid.ToString("F5", CultureInfo.InvariantCulture),
            " SAsk=",
            _slaveAsk.ToString("F5", CultureInfo.InvariantCulture)));
    }

    private void EmitExecutionLog(HftExecutionReport report)
    {
        Log?.Invoke(string.Concat(
            "[HFT] EXEC seq=",
            report.Seq.ToString(CultureInfo.InvariantCulture),
            " status=",
            report.Status.ToString(CultureInfo.InvariantCulture),
            " side=",
            report.Side.ToString(CultureInfo.InvariantCulture),
            " ticket=",
            report.Ticket.ToString(CultureInfo.InvariantCulture),
            " price=",
            report.FillPrice.ToString("F5", CultureInfo.InvariantCulture),
            " lots=",
            report.FillLots.ToString("F2", CultureInfo.InvariantCulture),
            " err=",
            report.ErrorCode.ToString(CultureInfo.InvariantCulture)));
    }

    private void EmitStateLog(string stateText)
    {
        var seq = _lastReportSeq;
        if (seq == _lastStateLogSeq && !string.Equals(stateText, "KILL", StringComparison.Ordinal))
        {
            return;
        }

        _lastStateLogSeq = seq;
        Log?.Invoke(string.Concat(
            "[HFT] STATE ",
            stateText,
            " side=",
            _positionSide.ToString(CultureInfo.InvariantCulture),
            " ticket=",
            _positionTicket.ToString(CultureInfo.InvariantCulture)));
    }
}

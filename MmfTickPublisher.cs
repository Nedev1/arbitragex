using System.IO.MemoryMappedFiles;
using System.Threading;

namespace ArbitrageX;

public sealed class MmfTickPublisher : System.IDisposable
{
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private long _masterSeq;
    private int _disposed;

    public MmfTickPublisher()
    {
        _mmf = MemoryMappedFile.CreateOrOpen(
            NexusMmfLayout.MapName,
            NexusMmfLayout.MapSize,
            MemoryMappedFileAccess.ReadWrite);
        _view = _mmf.CreateViewAccessor(0, NexusMmfLayout.MapSize, MemoryMappedFileAccess.ReadWrite);
        _masterSeq = _view.ReadInt64(NexusMmfLayout.MasterTickOffset + NexusMmfLayout.TickSeqOffset);
    }

    public void PublishMasterTick(double bid, double ask, long timestamp)
    {
        if (bid <= 0.0 || ask <= 0.0 || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        var baseOffset = NexusMmfLayout.MasterTickOffset;
        _view.Write(baseOffset + NexusMmfLayout.TickBidOffset, bid);
        _view.Write(baseOffset + NexusMmfLayout.TickAskOffset, ask);
        _view.Write(baseOffset + NexusMmfLayout.TickTimestampOffset, timestamp);
        _view.Write(baseOffset + NexusMmfLayout.TickSymbolHashOffset, 0);
        _view.Write(baseOffset + NexusMmfLayout.TickSourceHashOffset, 0);

        var seq = Interlocked.Increment(ref _masterSeq);
        _view.Write(baseOffset + NexusMmfLayout.TickSeqOffset, seq);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _view.Dispose();
        _mmf.Dispose();
    }
}

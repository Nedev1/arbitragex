using System;
using System.Threading;

namespace ArbitrageX;

public static class HftHash
{
    public static int GetHash(ReadOnlySpan<char> text)
    {
        unchecked
        {
            int hash = (int)2166136261;
            foreach (char c in text)
            {
                hash = (hash ^ char.ToUpperInvariant(c)) * 16777619;
            }
            return hash;
        }
    }

    public static int GetHash(string text) => GetHash(text.AsSpan());

    public static int GetSymbolHash(string text) => GetSymbolHash(text.AsSpan());

    public static int GetSymbolHash(ReadOnlySpan<char> text)
    {
        text = text.Trim();
        if (text.Length == 0)
        {
            return GetHash(text);
        }

        var pipeIdx = text.IndexOf('|');
        if (pipeIdx >= 0 && pipeIdx + 1 < text.Length)
        {
            text = text.Slice(pipeIdx + 1).Trim();
        }

        var dotIdx = text.IndexOf('.');
        if (dotIdx > 0)
        {
            text = text.Slice(0, dotIdx).Trim();
        }

        unchecked
        {
            int hash = (int)2166136261;
            var hadAlnum = false;
            for (var i = 0; i < text.Length; i++)
            {
                var c = text[i];
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
                {
                    hadAlnum = true;
                    hash = (hash ^ char.ToUpperInvariant(c)) * 16777619;
                }
            }

            if (hadAlnum)
            {
                return hash;
            }
        }

        return GetHash(text);
    }
}

public struct TickData // NOW A STRUCT FOR L1 CACHE LOCALITY
{
    public int TerminalHash;
    public int SymbolHash;
    public double Bid;
    public double Ask;
    public long Timestamp;
}

public class TickRingBuffer
{
    private readonly TickData[] _buffer;
    private readonly int _mask;
    private int _head;
    private int _tail;
    private SpinLock _spinLock = new SpinLock(false);

    public TickRingBuffer(int capacity = 65536)
    {
        _buffer = new TickData[capacity]; // Memory is contiguous
        _mask = capacity - 1;
    }

    public void Write(int terminalHash, int symbolHash, double bid, double ask, long timestamp)
    {
        bool lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);
            ref var tick = ref _buffer[_tail & _mask]; // By-ref update
            tick.TerminalHash = terminalHash;
            tick.SymbolHash = symbolHash;
            tick.Bid = bid;
            tick.Ask = ask;
            tick.Timestamp = timestamp;
            Volatile.Write(ref _tail, _tail + 1);
        }
        finally
        {
            if (lockTaken) _spinLock.Exit(false);
        }
    }

    public bool TryRead(out TickData tick)
    {
        if (_head < Volatile.Read(ref _tail))
        {
            tick = _buffer[_head & _mask]; // Struct copy by value (ultra fast)
            _head++;
            return true;
        }
        tick = default;
        return false;
    }
}

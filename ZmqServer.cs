using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Threading.Channels;
using NetMQ;
using NetMQ.Sockets;

namespace ArbitrageX;

public sealed class ZmqServer : IDisposable
{
    private readonly PullSocket _tickPull;
    private readonly PublisherSocket _commandPub;
    private readonly NetMQPoller _poller;
    private readonly Channel<TickMessage> _tickChannel;
    private readonly ConcurrentDictionary<string, byte> _terminals = new();

    public ChannelReader<TickMessage> Reader => _tickChannel.Reader;

    public ZmqServer()
    {
        _tickPull = new PullSocket();
        _commandPub = new PublisherSocket();
        _poller = new NetMQPoller { _tickPull };
        _tickChannel = Channel.CreateBounded<TickMessage>(new BoundedChannelOptions(2048)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });

        _tickPull.ReceiveReady += OnTick;
    }

    public void Start(string tickBind = "tcp://127.0.0.1:5557", string cmdBind = "tcp://127.0.0.1:5558")
    {
        _tickPull.Bind(tickBind);
        _commandPub.Bind(cmdBind);
        _poller.RunAsync();
    }

    public void PublishCommand(string command)
    {
        _commandPub.SendFrame(command);
    }

    public string[] GetTerminals()
    {
        return _terminals.Keys.ToArray();
    }

    private void OnTick(object? sender, NetMQSocketEventArgs e)
    {
        var msg = e.Socket.ReceiveFrameString();
        if (string.IsNullOrEmpty(msg))
        {
            return;
        }

        if (!TryParseTick(msg.AsSpan(), out var tick))
        {
            return;
        }

        _terminals.TryAdd(tick.TerminalId, 0);
        _tickChannel.Writer.TryWrite(tick);
    }

    private static bool TryParseTick(ReadOnlySpan<char> span, out TickMessage tick)
    {
        // Format: TICK|terminalId|symbol|bid|ask|timestamp
        tick = default;
        if (!TryReadToken(span, out var token, out var idx)) return false;
        if (!token.SequenceEqual("TICK")) return false;

        if (!TryReadToken(span, ref idx, out var terminal)) return false;
        if (!TryReadToken(span, ref idx, out var symbol)) return false;
        if (!TryReadToken(span, ref idx, out var bidSpan)) return false;
        if (!TryReadToken(span, ref idx, out var askSpan)) return false;
        if (!TryReadToken(span, ref idx, out var tsSpan)) return false;

        if (!double.TryParse(bidSpan, NumberStyles.Float, CultureInfo.InvariantCulture, out var bid)) return false;
        if (!double.TryParse(askSpan, NumberStyles.Float, CultureInfo.InvariantCulture, out var ask)) return false;
        if (!long.TryParse(tsSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ts)) ts = 0;

        tick = new TickMessage(terminal.ToString(), symbol.ToString(), bid, ask, ts);
        return true;
    }

    private static bool TryReadToken(ReadOnlySpan<char> span, out ReadOnlySpan<char> token, out int nextIndex)
    {
        var idx = span.IndexOf('|');
        if (idx < 0)
        {
            token = span;
            nextIndex = span.Length;
            return true;
        }
        token = span.Slice(0, idx);
        nextIndex = idx + 1;
        return true;
    }

    private static bool TryReadToken(ReadOnlySpan<char> span, ref int start, out ReadOnlySpan<char> token)
    {
        if (start >= span.Length)
        {
            token = default;
            return false;
        }

        var idx = span.Slice(start).IndexOf('|');
        if (idx < 0)
        {
            token = span.Slice(start);
            start = span.Length;
            return true;
        }

        token = span.Slice(start, idx);
        start += idx + 1;
        return true;
    }

    public void Dispose()
    {
        _poller.Stop();
        _tickPull.Dispose();
        _commandPub.Dispose();
        _poller.Dispose();
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArbitrageX;

public sealed class PipeServer : IDisposable
{
    private readonly Channel<FilledMessage> _filledChannel;
    private readonly Dictionary<string, PipeClient> _clients = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _clientsLock = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly string _pipeName;

    public TickRingBuffer Ticks { get; } = new TickRingBuffer();
    public ChannelReader<FilledMessage> Fills => _filledChannel.Reader;

    public event Action<string>? Log;
    public event Action<string>? TerminalConnected;
    public event Action<string>? PositionClosed;

    public string LastDebugMessage => _lastDebugMessage;
    private volatile string _lastDebugMessage = "Idle";

    public PipeServer(string pipeName = "TradePipe")
    {
        _pipeName = NormalizePipeName(pipeName);
        _filledChannel = Channel.CreateBounded<FilledMessage>(new BoundedChannelOptions(2048)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });
    }

    public void Start()
    {
        WriteLog($"Server started. Pipe name='{_pipeName}'");
        _ = Task.Run(AcceptLoop, _cts.Token);
    }

    public void PublishCommand(string command)
    {
        PipeClient[] snapshot;
        lock (_clientsLock)
        {
            snapshot = _clients.Values.ToArray();
        }

        foreach (var client in snapshot)
        {
            client.TrySend(command);
        }
    }

    public string[] GetTerminals()
    {
        lock (_clientsLock)
        {
            return _clients.Values
                .Select(c => c.TerminalId)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
    }

    public void Dispose()
    {
        _cts.Cancel();

        PipeClient[] snapshot;
        lock (_clientsLock)
        {
            snapshot = _clients.Values.ToArray();
            _clients.Clear();
        }

        foreach (var client in snapshot)
        {
            client.Dispose();
        }
    }

    private async Task AcceptLoop()
    {
        while (!_cts.IsCancellationRequested)
        {
            var server = new NamedPipeServerStream(
                _pipeName,
                PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances,
                PipeTransmissionMode.Message,
                PipeOptions.Asynchronous | PipeOptions.WriteThrough);

            try
            {
                WriteLog("Waiting for connection...");
                await server.WaitForConnectionAsync(_cts.Token).ConfigureAwait(false);
                WriteLog("Client connected!");
            }
            catch (Exception ex)
            {
                server.Dispose();
                if (_cts.IsCancellationRequested) return;
                WriteLog($"Error: {ex.Message}");
                try { await Task.Delay(1000, _cts.Token).ConfigureAwait(false); } catch { }
                continue;
            }

            var connectionId = Guid.NewGuid().ToString("N");
            var client = new PipeClient(
                connectionId,
                server,
                Ticks,
                _filledChannel.Writer,
                RegisterTerminal,
                RemoveClient,
                OnPositionClosedFromClient,
                WriteLog);

            lock (_clientsLock)
            {
                _clients[connectionId] = client;
            }

            _ = client.RunAsync();
        }
    }

    private void RegisterTerminal(PipeClient client, string terminalId)
    {
        var normalized = (terminalId ?? string.Empty).Trim();
        if (normalized.Length == 0) return;

        bool duplicate;
        lock (_clientsLock)
        {
            client.SetTerminalId(normalized);
            duplicate = _clients.Values.Any(existing =>
                !ReferenceEquals(existing, client) &&
                string.Equals(existing.TerminalId, normalized, StringComparison.OrdinalIgnoreCase));
        }

        if (duplicate)
        {
            WriteLog($"Warning: duplicate terminal id '{normalized}' connected. Configure unique TerminalId (e.g. MT5-02).");
        }

        WriteLog($"Terminal connected: {normalized} (conn={client.ConnectionId})");
        TerminalConnected?.Invoke(normalized);
    }

    private void RemoveClient(PipeClient client)
    {
        lock (_clientsLock)
        {
            _clients.Remove(client.ConnectionId);
        }

        if (client.TerminalId.Length > 0)
        {
            WriteLog($"Terminal disconnected: {client.TerminalId} (conn={client.ConnectionId})");
        }
        else
        {
            WriteLog($"Client disconnected: conn={client.ConnectionId}");
        }
    }

    private void OnPositionClosedFromClient(string terminalId)
    {
        var id = (terminalId ?? string.Empty).Trim();
        if (id.Length == 0) return;

        WriteLog($"CLOSED: terminal={id}");
        PositionClosed?.Invoke(id);
    }

    private void WriteLog(string message)
    {
        _lastDebugMessage = message;
        Console.WriteLine(message);
        Log?.Invoke(message);
    }

    private static string NormalizePipeName(string name)
    {
        const string marker = "\\\\.\\pipe\\";
        if (name.StartsWith(marker, StringComparison.OrdinalIgnoreCase))
        {
            return name.Substring(marker.Length);
        }

        return name;
    }

    private sealed class PipeClient : IDisposable
    {
        private readonly NamedPipeServerStream _stream;
        private readonly TickRingBuffer _tickBuffer;
        private readonly ChannelWriter<FilledMessage> _filledWriter;
        private readonly Action<PipeClient, string> _onIdentified;
        private readonly Action<PipeClient> _onDisconnected;
        private readonly Action<string> _onPositionClosed;
        private readonly Action<string> _log;
        private readonly byte[] _readBuffer = ArrayPool<byte>.Shared.Rent(2048);
        private readonly Channel<string> _writeChannel = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        private readonly CancellationTokenSource _cancel = new();
        private readonly Queue<string> _pendingLines = new();
        private readonly StringBuilder _pendingText = new StringBuilder();
        private long _lastTickStreamLogMs;
        private string _lastTickStreamSymbol = string.Empty;
        private int _disposed;
        private string _terminalId = string.Empty;

        public string ConnectionId { get; }
        public string TerminalId => _terminalId;

        public PipeClient(
            string connectionId,
            NamedPipeServerStream stream,
            TickRingBuffer tickBuffer,
            ChannelWriter<FilledMessage> filledWriter,
            Action<PipeClient, string> onIdentified,
            Action<PipeClient> onDisconnected,
            Action<string> onPositionClosed,
            Action<string> log)
        {
            ConnectionId = connectionId;
            _stream = stream;
            _tickBuffer = tickBuffer;
            _filledWriter = filledWriter;
            _onIdentified = onIdentified;
            _onDisconnected = onDisconnected;
            _onPositionClosed = onPositionClosed;
            _log = log;
            _ = Task.Run(WriteLoopAsync);
        }

        public async Task RunAsync()
        {
            try
            {
                while (_stream.IsConnected)
                {
                    var message = await ReadMessageAsync().ConfigureAwait(false);
                    if (message is null) break;
                    if (message.Length == 0) continue;

                    if (message.StartsWith("HELLO|", StringComparison.Ordinal))
                    {
                        var requestedId = message.Substring(6).Trim();
                        Identify(requestedId);
                        continue;
                    }

                    if (message.StartsWith("TICK|", StringComparison.Ordinal))
                    {
                        if (TryParseRawTick(message.AsSpan(), out var rawTick))
                        {
                            var nowMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
                            if (nowMs - _lastTickStreamLogMs >= 5000
                                || !string.Equals(_lastTickStreamSymbol, rawTick.Symbol, StringComparison.OrdinalIgnoreCase))
                            {
                                _lastTickStreamLogMs = nowMs;
                                _lastTickStreamSymbol = rawTick.Symbol;
                                _log(
                                    $"Tick stream: terminal={rawTick.TerminalId} symbol={rawTick.Symbol} " +
                                    $"bid={rawTick.Bid:F5} ask={rawTick.Ask:F5}");
                            }

                            _tickBuffer.Write(rawTick.TerminalHash, rawTick.SymbolHash, rawTick.Bid, rawTick.Ask, rawTick.Timestamp);
                        }

                        continue;
                    }

                    if (message.StartsWith("FILLED|", StringComparison.Ordinal))
                    {
                        if (TryParseRawFill(message.AsSpan(), out var rawFill))
                        {
                            var effectiveId = rawFill.TerminalId.Trim();
                            if (effectiveId.Length == 0)
                            {
                                effectiveId = _terminalId;
                            }

                            if (effectiveId.Length == 0)
                            {
                                continue;
                            }

                            if (_terminalId.Length == 0)
                            {
                                Identify(effectiveId);
                            }

                            var fill = new FilledMessage(
                                effectiveId,
                                rawFill.Symbol,
                                rawFill.Side,
                                rawFill.Lots,
                                rawFill.Price,
                                rawFill.Sl,
                                rawFill.Tp,
                                rawFill.Ticket);

                            _filledWriter.TryWrite(fill);
                            _log($"FILLED: terminal={fill.TerminalId} symbol={fill.Symbol} side={fill.Side} lots={fill.Lots:F2} price={fill.Price:F5} ticket={fill.Ticket}");
                        }

                        continue;
                    }

                    if (message.StartsWith("ERROR|", StringComparison.Ordinal))
                    {
                        if (TryParseRawError(message.AsSpan(), out var rawError))
                        {
                            var effectiveId = rawError.TerminalId.Trim();
                            if (effectiveId.Length == 0)
                            {
                                effectiveId = _terminalId;
                            }

                            if (effectiveId.Length == 0)
                            {
                                continue;
                            }

                            if (_terminalId.Length == 0)
                            {
                                Identify(effectiveId);
                            }

                            var errorAsFill = new FilledMessage(
                                effectiveId,
                                rawError.Symbol,
                                rawError.Reason,
                                0.0,
                                0.0,
                                0.0,
                                0.0,
                                rawError.ErrorCode);

                            _filledWriter.TryWrite(errorAsFill);
                            _log(
                                $"ERROR: terminal={effectiveId} symbol={rawError.Symbol} reason={rawError.Reason} code={rawError.ErrorCode}");
                        }

                        continue;
                    }

                    if (message.StartsWith("CLOSED|", StringComparison.Ordinal))
                    {
                        var id = message.Length > 7 ? message.Substring(7).Trim() : string.Empty;
                        if (id.Length == 0)
                        {
                            id = _terminalId;
                        }

                        if (id.Length > 0)
                        {
                            if (_terminalId.Length == 0)
                            {
                                Identify(id);
                            }

                            _onPositionClosed(id);
                        }

                        continue;
                    }
                }
            }
            catch (Exception ex)
            {
                _log($"Error: {ex.Message}");
            }
            finally
            {
                Dispose();
                _onDisconnected(this);
            }
        }

        private void Identify(string terminalId)
        {
            var normalized = (terminalId ?? string.Empty).Trim();
            if (normalized.Length == 0) return;

            if (_terminalId.Length == 0)
            {
                _terminalId = normalized;
                _onIdentified(this, normalized);
                return;
            }

            _terminalId = normalized;
        }

        public void SetTerminalId(string terminalId)
        {
            _terminalId = (terminalId ?? string.Empty).Trim();
        }

        public void TrySend(string message)
        {
            if (!_stream.IsConnected) return;
            _writeChannel.Writer.TryWrite(message);
        }

        private async Task WriteLoopAsync()
        {
            try
            {
                while (_stream.IsConnected && !_cancel.IsCancellationRequested)
                {
                    var message = await _writeChannel.Reader.ReadAsync(_cancel.Token).ConfigureAwait(false);
                    var bytes = Encoding.ASCII.GetBytes(message + "\n");
                    await _stream.WriteAsync(bytes, 0, bytes.Length, _cancel.Token).ConfigureAwait(false);
                }
            }
            catch
            {
                // disconnected/broken pipe
            }
        }

        private async Task<string?> ReadMessageAsync()
        {
            if (_pendingLines.Count > 0)
            {
                return _pendingLines.Dequeue();
            }

            while (_stream.IsConnected)
            {
                int bytesRead;
                try
                {
                    bytesRead = await _stream.ReadAsync(_readBuffer, 0, _readBuffer.Length, _cancel.Token).ConfigureAwait(false);
                }
                catch
                {
                    return null;
                }

                if (bytesRead == 0) return null;

                _pendingText.Append(Encoding.ASCII.GetString(_readBuffer, 0, bytesRead));
                var textSpan = _pendingText.ToString();
                var splitIndex = textSpan.IndexOf('\n');

                while (splitIndex >= 0)
                {
                    var line = textSpan.Substring(0, splitIndex).Trim();
                    textSpan = splitIndex + 1 < textSpan.Length
                        ? textSpan.Substring(splitIndex + 1)
                        : string.Empty;

                    if (line.Length > 0)
                    {
                        _pendingLines.Enqueue(line);
                    }

                    splitIndex = textSpan.IndexOf('\n');
                }

                _pendingText.Clear();
                _pendingText.Append(textSpan);

                if (_pendingLines.Count > 0)
                {
                    return _pendingLines.Dequeue();
                }
            }

            return null;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try { _writeChannel.Writer.TryComplete(); } catch { }
            try { _cancel.Cancel(); } catch { }
            try { _stream.Dispose(); } catch { }
            ArrayPool<byte>.Shared.Return(_readBuffer);
        }
    }

    private static bool TryParseRawTick(ReadOnlySpan<char> span, out RawTick raw)
    {
        raw = default;
        if (!TryReadToken(ref span, out var kind, requireDelimiter: true)) return false;
        if (!kind.Equals("TICK".AsSpan(), StringComparison.Ordinal)) return false;

        if (!TryReadToken(ref span, out var terminalId, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var symbol, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var bidToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var askToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var tsToken, requireDelimiter: false)) return false;

        if (!TryParseProtocolDouble(bidToken, out var bid)) return false;
        if (!TryParseProtocolDouble(askToken, out var ask)) return false;
        TryParseProtocolLong(tsToken, out _);
        var arrivalMs = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);

        var terminalIdText = SpanToTrimmedString(terminalId);
        var symbolText = SpanToTrimmedString(symbol);
        int termHash = HftHash.GetHash(terminalIdText);
        int symHash = HftHash.GetSymbolHash(symbolText);

        raw = new RawTick(
            termHash,
            symHash,
            bid,
            ask,
            arrivalMs,
            terminalIdText,
            symbolText);

        return true;
    }

    private static bool TryParseRawError(ReadOnlySpan<char> span, out RawError raw)
    {
        raw = default;
        if (!TryReadToken(ref span, out var kind, requireDelimiter: true)) return false;
        if (!kind.Equals("ERROR".AsSpan(), StringComparison.Ordinal)) return false;

        if (!TryReadToken(ref span, out var terminalId, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var symbol, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var reason, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var codeToken, requireDelimiter: false)) return false;

        if (!TryParseProtocolLong(codeToken, out var code)) return false;

        raw = new RawError(
            SpanToTrimmedString(terminalId),
            SpanToTrimmedString(symbol),
            SpanToTrimmedString(reason),
            code);

        return true;
    }

    private static bool TryParseRawFill(ReadOnlySpan<char> span, out RawFill raw)
    {
        raw = default;
        if (!TryReadToken(ref span, out var kind, requireDelimiter: true)) return false;
        if (!kind.Equals("FILLED".AsSpan(), StringComparison.Ordinal)) return false;

        if (!TryReadToken(ref span, out var terminalId, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var symbol, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var side, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var lotsToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var priceToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var slToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var tpToken, requireDelimiter: true)) return false;
        if (!TryReadToken(ref span, out var ticketToken, requireDelimiter: false)) return false;

        if (!TryParseProtocolDouble(lotsToken, out var lots)) return false;
        if (!TryParseProtocolDouble(priceToken, out var price)) return false;
        if (!TryParseProtocolDouble(slToken, out var sl)) return false;
        if (!TryParseProtocolDouble(tpToken, out var tp)) return false;
        TryParseProtocolLong(ticketToken, out var ticket);

        raw = new RawFill(
            SpanToTrimmedString(terminalId),
            SpanToTrimmedString(symbol),
            SpanToTrimmedString(side),
            lots,
            price,
            sl,
            tp,
            ticket);

        return true;
    }

    private readonly struct RawTick
    {
        public readonly int TerminalHash;
        public readonly int SymbolHash;
        public readonly double Bid;
        public readonly double Ask;
        public readonly long Timestamp;
        public readonly string TerminalId;
        public readonly string Symbol;

        public RawTick(int terminalHash, int symbolHash, double bid, double ask, long timestamp, string terminalId, string symbol)
        {
            TerminalHash = terminalHash;
            SymbolHash = symbolHash;
            Bid = bid;
            Ask = ask;
            Timestamp = timestamp;
            TerminalId = terminalId;
            Symbol = symbol;
        }
    }

    private readonly struct RawFill
    {
        public readonly string TerminalId;
        public readonly string Symbol;
        public readonly string Side;
        public readonly double Lots;
        public readonly double Price;
        public readonly double Sl;
        public readonly double Tp;
        public readonly long Ticket;

        public RawFill(string terminalId, string symbol, string side, double lots, double price, double sl, double tp, long ticket)
        {
            TerminalId = terminalId;
            Symbol = symbol;
            Side = side;
            Lots = lots;
            Price = price;
            Sl = sl;
            Tp = tp;
            Ticket = ticket;
        }
    }

    private readonly struct RawError
    {
        public readonly string TerminalId;
        public readonly string Symbol;
        public readonly string Reason;
        public readonly long ErrorCode;

        public RawError(string terminalId, string symbol, string reason, long errorCode)
        {
            TerminalId = terminalId;
            Symbol = symbol;
            Reason = reason;
            ErrorCode = errorCode;
        }
    }

    private static bool TryReadToken(ref ReadOnlySpan<char> source, out ReadOnlySpan<char> token, bool requireDelimiter)
    {
        if (source.Length == 0)
        {
            token = default;
            return false;
        }

        var idx = source.IndexOf('|');
        if (idx < 0)
        {
            if (requireDelimiter)
            {
                token = default;
                return false;
            }

            token = source;
            source = ReadOnlySpan<char>.Empty;
            return true;
        }

        token = source.Slice(0, idx);
        source = source.Slice(idx + 1);
        return true;
    }

    private static string SpanToTrimmedString(ReadOnlySpan<char> value)
    {
        return value.Trim().ToString();
    }

    private static bool TryParseProtocolLong(ReadOnlySpan<char> value, out long result)
    {
        result = 0;
        value = value.Trim();
        if (value.Length == 0) return false;

        bool isNegative = false;
        int start = 0;
        if (value[0] == '-')
        {
            isNegative = true;
            start = 1;
        }

        for (int i = start; i < value.Length; i++)
        {
            char c = value[i];
            if (c < '0' || c > '9') return false;
            result = result * 10 + (c - '0');
        }

        if (isNegative) result = -result;
        return true;
    }

    private static bool TryParseProtocolDouble(ReadOnlySpan<char> value, out double result)
    {
        result = 0.0;
        value = value.Trim();
        if (value.Length == 0) return false;

        bool isNegative = false;
        int start = 0;
        if (value[0] == '-')
        {
            isNegative = true;
            start = 1;
        }

        long integerPart = 0;
        long fractionalPart = 0;
        int decimalPlaces = 0;
        bool inFraction = false;

        for (int i = start; i < value.Length; i++)
        {
            char c = value[i];
            if (c == '.' || c == ',')
            {
                if (inFraction) return false; // multiple decimals
                inFraction = true;
                continue;
            }

            if (c < '0' || c > '9') return false;

            if (inFraction)
            {
                fractionalPart = fractionalPart * 10 + (c - '0');
                decimalPlaces++;
            }
            else
            {
                integerPart = integerPart * 10 + (c - '0');
            }
        }

        double frac = fractionalPart;
        for (int i = 0; i < decimalPlaces; i++)
        {
            frac /= 10.0;
        }

        result = integerPart + frac;
        if (isNegative) result = -result;
        return true;
    }
}

public readonly struct FilledMessage
{
    public readonly string TerminalId;
    public readonly string Symbol;
    public readonly string Side;
    public readonly double Lots;
    public readonly double Price;
    public readonly double Sl;
    public readonly double Tp;
    public readonly long Ticket;

    public FilledMessage(string terminalId, string symbol, string side, double lots, double price, double sl, double tp, long ticket)
    {
        TerminalId = terminalId;
        Symbol = symbol;
        Side = side;
        Lots = lots;
        Price = price;
        Sl = sl;
        Tp = tp;
        Ticket = ticket;
    }
}

using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArbitrageX;

public static class AsyncLogger
{
    private static readonly Channel<string> _channel = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    private static string _filePath = "debug.log";
    private static int _started;

    public static void Start(string filePath)
    {
        _filePath = filePath;
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            return;
        }

        _ = Task.Run(WriteLoopAsync);
    }

    public static void Enqueue(string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} {message}{Environment.NewLine}";
        _channel.Writer.TryWrite(line);
    }

    private static async Task WriteLoopAsync()
    {
        while (await _channel.Reader.WaitToReadAsync().ConfigureAwait(false))
        {
            while (_channel.Reader.TryRead(out var line))
            {
                try
                {
                    File.AppendAllText(_filePath, line);
                }
                catch
                {
                    // ignore disk locks/errors
                }
            }
        }
    }
}

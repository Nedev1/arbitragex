using System;
using System.IO;
using System.IO.Pipes;
using System.Text;
using cAlgo.API;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class Bridge : Robot
    {
        [Parameter("Terminal Id", DefaultValue = "CTRADER-01")]
        public string TerminalId { get; set; }

        [Parameter("Master Symbol", DefaultValue = "NAS100")]
        public string MasterSymbol { get; set; }

        [Parameter("Pipe Name", DefaultValue = "ArbitrageXPipe")]
        public string PipeName { get; set; }

        private NamedPipeClientStream _pipe;
        private StreamWriter _writer;
        private readonly byte[] _rxBuffer = new byte[512];
        private readonly StringBuilder _rx = new StringBuilder(512);
        private System.Threading.Tasks.Task<int> _pendingRead;

        protected override void OnStart()
        {
            ConnectPipe();
            SendLine($"HELLO|{TerminalId}");
        }

        protected override void OnTick()
        {
            if (SymbolName != MasterSymbol)
                return;

            if (_pipe == null || !_pipe.IsConnected)
                ConnectPipe();

            var bid = Symbol.Bid;
            var ask = Symbol.Ask;
            var ts = Server.TimeInMilliseconds;

            SendLine($"TICK|{TerminalId}|{SymbolName}|{bid:F5}|{ask:F5}|{ts}");
        }

        protected override void OnTimer()
        {
            if (_pipe == null || !_pipe.IsConnected)
                return;

            if (_pendingRead == null)
                _pendingRead = _pipe.ReadAsync(_rxBuffer, 0, _rxBuffer.Length);

            if (!_pendingRead.IsCompleted)
                return;

            var read = _pendingRead.Result;
            _pendingRead = null;
            if (read <= 0)
                return;

            var chunk = Encoding.ASCII.GetString(_rxBuffer, 0, read);
            _rx.Append(chunk);

            while (true)
            {
                var idx = _rx.ToString().IndexOf('\n');
                if (idx < 0)
                    break;

                var line = _rx.ToString(0, idx).Trim();
                _rx.Remove(0, idx + 1);
                if (line.Length > 0)
                    ProcessCommand(line);
            }
        }

        private void ConnectPipe()
        {
            try
            {
                _pipe?.Dispose();
                _pipe = new NamedPipeClientStream(".", PipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
                _pipe.Connect(2000);
                _writer = new StreamWriter(_pipe, Encoding.ASCII, 256, true) { AutoFlush = true };
                Timer.Start(0.01);
            }
            catch
            {
                // ignore connect failures, will retry on next tick
            }
        }

        private void SendLine(string line)
        {
            try
            {
                _writer?.WriteLine(line);
            }
            catch
            {
                // ignored
            }
        }

        private void ProcessCommand(string cmd)
        {
            if (cmd.StartsWith("KILL|", StringComparison.Ordinal))
            {
                foreach (var pos in Positions)
                    ClosePosition(pos);
                return;
            }

            var parts = cmd.Split('|');
            if (parts.Length < 7)
                return;

            var terminal = parts[1];
            if (!terminal.Equals(TerminalId, StringComparison.OrdinalIgnoreCase))
                return;

            var symbol = parts[2];
            var side = parts[3];
            var lots = double.Parse(parts[4]);
            var slReq = double.Parse(parts[5]);
            var tpReq = double.Parse(parts[6]);

            var volume = Symbol.QuantityToVolumeInUnits(lots);
            var tradeType = side == "BUY" ? TradeType.Buy : TradeType.Sell;
            var result = ExecuteMarketOrder(tradeType, symbol, volume, "ArbitrageX", slReq, tpReq);
            if (!result.IsSuccessful)
                return;

            WatchdogModify(result.Position, slReq, tpReq);
            SendLine($"FILLED|{TerminalId}|{symbol}|{side}|{lots:F2}|{slReq:F2}|{tpReq:F2}|{result.Position.Id}");
        }

        private void WatchdogModify(Position pos, double slReq, double tpReq)
        {
            if (pos == null)
                return;

            var sl = pos.StopLoss;
            var tp = pos.TakeProfit;
            if (sl != slReq || tp != tpReq)
            {
                ModifyPosition(pos, slReq, tpReq);
            }
        }
    }
}

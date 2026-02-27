#property strict
#property description "Bridge (MT5 Named Pipes - Universal)"

#include <Trade/Trade.mqh>

#define GENERIC_READ 0x80000000
#define GENERIC_WRITE 0x40000000
#define OPEN_EXISTING 3
#define PIPE_READMODE_MESSAGE 0x00000002
#define MODIFY_RETRY_INTERVAL_MS 250
#define MAX_MODIFY_RETRIES 40
#define COMMAND_TIMER_INTERVAL_MS 25
#define MAX_COMMANDS_PER_PUMP 64
#define PIPE_READ_CHUNK_BYTES 2048
#define TRADE_CMD_MAX_AGE_MS 50

#import "kernel32.dll"
long CreateFileW(string name, uint access, uint share, long security, uint creation, uint flags, long hTemplate);
bool ReadFile(long handle, uchar& buffer[], uint bytesToRead, uint& bytesRead, long overlapped);
bool WriteFile(long handle, uchar& buffer[], uint bytesToWrite, uint& bytesWritten, long overlapped);
bool FlushFileBuffers(long handle);
bool CloseHandle(long handle);
bool WaitNamedPipeW(string name, uint timeout);
bool SetNamedPipeHandleState(long handle, uint& mode, uint maxcoll, uint maxtime);
bool PeekNamedPipe(long handle, uchar& buffer[], uint buflen, uint& bytesRead, uint& bytesAvail, uint& bytesLeft);
void GetSystemTimeAsFileTime(long& filetime[]);
#import

input string TerminalId = "MT5-01";
input string PipeName = "\\\\.\\pipe\\TradePipe";
input bool VerboseLogs = false;
input int MaxSlippagePoints = 15;
input bool PreferFokFilling = false; // false=IOC, true=FOK

long g_pipe = -1;
CTrade g_trade;
string g_terminalId;
ulong g_lastReportedTicket = 0;
string g_pipeReadBuffer = "";

struct PendingModification
{
   ulong Ticket;
   string Symbol;
   double ExpectedSl;
   double ExpectedTp;
   int Attempts;
   ulong LastAttemptMs;
};

PendingModification g_pendingModifications[];

int OnInit()
{
   g_terminalId = TerminalId;
   g_trade.SetDeviationInPoints(10);
   g_trade.SetTypeFilling(ORDER_FILLING_IOC);
   // Fire order requests without blocking on broker matching latency.
   g_trade.SetAsyncMode(true);
   EventSetMillisecondTimer(COMMAND_TIMER_INTERVAL_MS);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if (g_pipe != -1) CloseHandle(g_pipe);
   g_pipeReadBuffer = "";
   ArrayResize(g_pendingModifications, 0);
}

void OnTick()
{
   static int prevPositions = -1;
   int currentPositions = PositionsTotal();

   // Persistent watchdog: keep applying expected SL/TP until confirmed.
   ProcessPendingModifications();
   PumpPipeCommands();

   if (currentPositions == 0)
   {
      g_lastReportedTicket = 0;
   }

   // Async safety: if broker confirms position later, emit FILLED once here.
   ReportFilledIfNeeded();

   if (prevPositions >= 0 && prevPositions > 0 && currentPositions == 0 && g_pipe != -1)
   {
      SendLine("CLOSED|" + g_terminalId);
   }

   if (g_pipe != -1)
   {
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      ulong ts = GetMicrosecondCount();

      string msg = "TICK|" + g_terminalId + "|" + _Symbol + "|" + DoubleToString(bid, 5) + "|" + DoubleToString(ask, 5) + "|" + IntegerToString(ts);
      SendLine(msg);
   }

   prevPositions = currentPositions;
}

void OnTrade()
{
   // Trade events arrive faster than quote ticks under some brokers.
   ReportFilledIfNeeded();
   PumpPipeCommands();
}

void OnTimer()
{
   // Retry pending SL/TP modifications even if no new ticks arrive.
   ProcessPendingModifications();

   if (g_pipe == -1)
   {
      if (ConnectPipe())
      {
         Print("Connected to Pipe!");
         SendLine("HELLO|" + g_terminalId);
      }
      return;
   }

   PumpPipeCommands();
}

bool ConnectPipe()
{
   if (!WaitNamedPipeW(PipeName, 2000)) return false;

   uint access = (uint)(GENERIC_READ | GENERIC_WRITE);
   g_pipe = CreateFileW(PipeName, access, 0, 0, OPEN_EXISTING, 0, 0);
   if (g_pipe == -1) return false;

   uint mode = PIPE_READMODE_MESSAGE;
   SetNamedPipeHandleState(g_pipe, mode, 0, 0);
   g_pipeReadBuffer = "";
   return true;
}

long CurrentUtcMilliseconds()
{
   long filetime[1];
   filetime[0] = 0;
   GetSystemTimeAsFileTime(filetime);

   ulong ticks100ns = (ulong)filetime[0];
   const ulong EPOCH_1601_TO_1970_100NS = 116444736000000000;
   if (ticks100ns <= EPOCH_1601_TO_1970_100NS)
   {
      return 0;
   }

   return (long)((ticks100ns - EPOCH_1601_TO_1970_100NS) / 10000);
}

bool SendLine(string line)
{
   if (g_pipe == -1) return false;

   string payload = line + "\n";
   uchar data[];
   StringToCharArray(payload, data);
   int len = ArraySize(data) - 1; // skip null terminator
   if (len <= 0) return false;

   uint written = 0;
   if (VerboseLogs) Print("WriteFile: bytes=", len);
   bool ok = WriteFile(g_pipe, data, (uint)len, written, 0);
   if (!ok)
   {
      if (VerboseLogs) Print("WriteFile failed, resetting pipe handle");
      if (g_pipe != -1) CloseHandle(g_pipe);
      g_pipe = -1;
      g_pipeReadBuffer = "";
      return false;
   }

   // Avoid per-tick flush; it is expensive and can stall MT5 under high flow.
   if (VerboseLogs) FlushFileBuffers(g_pipe);
   return true;
}

string ReadLine()
{
   if (g_pipe == -1) return "";

   uchar peek[];
   ArrayResize(peek, 1);
   uint bytesRead = 0, bytesAvail = 0, bytesLeft = 0;
   if (!PeekNamedPipe(g_pipe, peek, 0, bytesRead, bytesAvail, bytesLeft))
   {
      if (VerboseLogs) Print("PeekNamedPipe failed, resetting pipe handle");
      if (g_pipe != -1) CloseHandle(g_pipe);
      g_pipe = -1;
      g_pipeReadBuffer = "";
      return "";
   }

   string line = "";
   while (TryPopBufferedLine(line))
   {
      if (line != "") return line;
   }

   if (bytesAvail == 0) return "";

   uint bytesToRead = bytesAvail;
   if (bytesToRead > PIPE_READ_CHUNK_BYTES) bytesToRead = PIPE_READ_CHUNK_BYTES;
   if (bytesToRead == 0) bytesToRead = 1;

   uchar buffer[];
   ArrayResize(buffer, (int)bytesToRead);
   uint read = 0;
   if (VerboseLogs) Print("ReadFile: bytesAvail=", bytesAvail, " bytesToRead=", bytesToRead);
   bool ok = ReadFile(g_pipe, buffer, bytesToRead, read, 0);
   if (!ok || read == 0)
   {
      if (!ok)
      {
         if (VerboseLogs) Print("ReadFile failed, resetting pipe handle");
         if (g_pipe != -1) CloseHandle(g_pipe);
         g_pipe = -1;
         g_pipeReadBuffer = "";
      }
      return "";
   }

   g_pipeReadBuffer += CharArrayToString(buffer, 0, (int)read);
   while (TryPopBufferedLine(line))
   {
      if (line != "") return line;
   }

   return "";
}

bool TryPopBufferedLine(string &line)
{
   line = "";
   int idx = StringFind(g_pipeReadBuffer, "\n");
   if (idx < 0) return false;

   line = StringSubstr(g_pipeReadBuffer, 0, idx);
   int next = idx + 1;
   int total = StringLen(g_pipeReadBuffer);
   if (next < total)
   {
      g_pipeReadBuffer = StringSubstr(g_pipeReadBuffer, next);
   }
   else
   {
      g_pipeReadBuffer = "";
   }

   StringTrimLeft(line);
   StringTrimRight(line);
   return true;
}

void PumpPipeCommands()
{
   if (g_pipe == -1) return;

   for (int i = 0; i < MAX_COMMANDS_PER_PUMP; i++)
   {
      string cmd = ReadLine();
      if (cmd == "") break;
      ProcessCommand(cmd);
   }
}

void ReportFilledIfNeeded()
{
   if (g_pipe == -1) return;
   if (PositionsTotal() <= 0) return;
   if (!PositionSelect(_Symbol)) return;

   ulong ticket = (ulong)PositionGetInteger(POSITION_TICKET);
   if (ticket == 0 || ticket == g_lastReportedTicket) return;

   ENUM_POSITION_TYPE ptype = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   string side = (ptype == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   double lots = PositionGetDouble(POSITION_VOLUME);
   double fillPrice = PositionGetDouble(POSITION_PRICE_OPEN);
   double sl = PositionGetDouble(POSITION_SL);
   double tp = PositionGetDouble(POSITION_TP);
   string symbol = PositionGetString(POSITION_SYMBOL);
   if (symbol == "") symbol = _Symbol;
   int digits = DigitsForSymbol(symbol);

   SendLine("FILLED|" + g_terminalId + "|" + symbol + "|" + side + "|" +
            DoubleToString(lots, 2) + "|" +
            DoubleToString(fillPrice, digits) + "|" +
            DoubleToString(sl, digits) + "|" +
            DoubleToString(tp, digits) + "|" +
            IntegerToString((long)ticket));

   g_lastReportedTicket = ticket;
}

void ProcessCommand(string cmd)
{
   if (StringFind(cmd, "KILL|") == 0)
   {
      string killParts[];
      int killCount = StringSplit(cmd, '|', killParts);
      if (killCount < 2)
      {
         CloseAllPositions();
         return;
      }

      string killTerminal = killParts[1];
      StringTrimLeft(killTerminal);
      StringTrimRight(killTerminal);
      string killTerminalNorm = NormalizeText(killTerminal);
      if (killTerminalNorm == "ALL")
      {
         CloseAllPositions();
         return;
      }

      if (killTerminalNorm != NormalizeText(g_terminalId))
      {
         return;
      }

      string killSymbol = "";
      if (killCount >= 3)
      {
         killSymbol = killParts[2];
         StringTrimLeft(killSymbol);
         StringTrimRight(killSymbol);
      }

      if (killSymbol == "" || NormalizeText(killSymbol) == "ALL")
      {
         CloseAllPositions();
      }
      else
      {
         ClosePositionsBySymbol(killSymbol);
      }
      return;
   }

   if (StringFind(cmd, "RENAME|") == 0)
   {
      string newId = StringSubstr(cmd, 7);
      StringTrimLeft(newId);
      StringTrimRight(newId);
      if (newId != "")
      {
         g_terminalId = newId;
         Print("TerminalId renamed to: ", g_terminalId);
      }
      return;
   }

   if (StringFind(cmd, "MODIFY|") == 0)
   {
      // MODIFY|terminal|symbol|ticket|sl|tp
      string modParts[];
      int modCount = StringSplit(cmd, '|', modParts);
      if (modCount < 6) return;

      string terminal = modParts[1];
      string symbol = modParts[2];
      ulong ticket = (ulong)StringToInteger(modParts[3]);
      double slPrice = StringToDouble(modParts[4]);
      double tpPrice = StringToDouble(modParts[5]);

      if (NormalizeText(terminal) != NormalizeText(g_terminalId)) return;

      string actualSymbol = symbol;
      ulong actualTicket = ticket;
      ENUM_POSITION_TYPE side = POSITION_TYPE_BUY;
      bool hasPosition = false;

      if (ticket > 0 && PositionSelectByTicket(ticket))
      {
         hasPosition = true;
         actualTicket = (ulong)PositionGetInteger(POSITION_TICKET);
         actualSymbol = PositionGetString(POSITION_SYMBOL);
         side = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      }
      else if (symbol != "" && PositionSelect(symbol))
      {
         hasPosition = true;
         actualTicket = (ulong)PositionGetInteger(POSITION_TICKET);
         actualSymbol = PositionGetString(POSITION_SYMBOL);
         side = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      }

      if (!hasPosition || actualTicket == 0) return;

      int digits = DigitsForSymbol(actualSymbol);
      if (slPrice > 0.0) slPrice = NormalizeDouble(slPrice, digits);
      if (tpPrice > 0.0) tpPrice = NormalizeDouble(tpPrice, digits);
      NormalizeStopsForPosition(actualSymbol, side, slPrice, tpPrice);

      bool modified = g_trade.PositionModify(actualTicket, slPrice, tpPrice);
      if (!modified)
      {
         Print("Failed to modify position: retcode=", g_trade.ResultRetcode(), " ",
               g_trade.ResultRetcodeDescription(),
               " terminal=", terminal,
               " symbol=", actualSymbol,
               " ticket=", actualTicket,
               " sl=", DoubleToString(slPrice, digits),
               " tp=", DoubleToString(tpPrice, digits));
         bool closed = g_trade.PositionClose(actualTicket);
         if (!closed)
         {
            Print("[FALLBACK] Modify rejected by broker. Position close failed. retcode=", g_trade.ResultRetcode(), " ",
                  g_trade.ResultRetcodeDescription(),
                  " ticket=", actualTicket,
                  " symbol=", actualSymbol);
         }
         else
         {
            Print("[FALLBACK] Modify rejected by broker. Position closed for safety.");
         }
      }
      else if (VerboseLogs)
      {
         Print("Position modified: terminal=", terminal,
               " symbol=", actualSymbol,
               " ticket=", actualTicket,
               " sl=", DoubleToString(slPrice, digits),
               " tp=", DoubleToString(tpPrice, digits));
      }

      return;
   }

   // TRADE|terminal|symbol|side|lots|sl|tp|timestampMsUtc (SL/TP are absolute prices)
   string parts[];
   int n = StringSplit(cmd, '|', parts);
   if (n < 8) return;

   string terminal = parts[1];
   string symbol = parts[2];
   string side = parts[3];
   double lots = StringToDouble(parts[4]);
   double slPrice = StringToDouble(parts[5]);
   double tpPrice = StringToDouble(parts[6]);
   long commandTimestampMs = (long)StringToInteger(parts[7]);

   if (NormalizeText(terminal) != NormalizeText(g_terminalId)) return;

   if (commandTimestampMs > 0)
   {
      long nowMs = CurrentUtcMilliseconds();
      long commandAgeMs = nowMs - commandTimestampMs;
      if (commandAgeMs > TRADE_CMD_MAX_AGE_MS)
      {
         if (VerboseLogs)
         {
            Print("Ignoring stale TRADE command. ageMs=", commandAgeMs,
                  " maxAgeMs=", TRADE_CMD_MAX_AGE_MS,
                  " symbol=", symbol, " side=", side);
         }
         return;
      }
   }

   int digits = DigitsForSymbol(symbol);
   if (slPrice > 0.0) slPrice = NormalizeDouble(slPrice, digits);
   if (tpPrice > 0.0) tpPrice = NormalizeDouble(tpPrice, digits);
   double sendSl = slPrice;
   double sendTp = tpPrice;
   NormalizeStopsForTradeSide(symbol, side, sendSl, sendTp);

   bool opened = false;
   ConfigureTradeExecution(symbol);
   if (side == "BUY")
   {
      // Inject absolute SL/TP directly into initial execution.
      opened = g_trade.Buy(lots, symbol, 0.0, sendSl, sendTp, "");
   }
   else if (side == "SELL")
   {
      opened = g_trade.Sell(lots, symbol, 0.0, sendSl, sendTp, "");
   }
   else
   {
      Print("Unknown trade side: ", side);
      return;
   }

   long openRetcode = g_trade.ResultRetcode();
   if (!opened)
   {
      bool retryWithoutStops = (sendSl > 0.0 || sendTp > 0.0) &&
                               (openRetcode == TRADE_RETCODE_INVALID_STOPS ||
                                openRetcode == TRADE_RETCODE_INVALID_PRICE ||
                                openRetcode == TRADE_RETCODE_PRICE_OFF ||
                                openRetcode == TRADE_RETCODE_REQUOTE);
      if (retryWithoutStops)
      {
         if (VerboseLogs)
         {
            Print("Entry rejected (retcode=", openRetcode, "). Retrying without SL/TP.");
         }

         if (side == "BUY")
         {
            opened = g_trade.Buy(lots, symbol, 0.0, 0.0, 0.0, "");
         }
         else
         {
            opened = g_trade.Sell(lots, symbol, 0.0, 0.0, 0.0, "");
         }

         openRetcode = g_trade.ResultRetcode();
      }
   }

   if (!opened)
   {
      Print("Trade open failed: retcode=", openRetcode, " ",
            g_trade.ResultRetcodeDescription(),
            " symbol=", symbol, " side=", side,
            " lots=", DoubleToString(lots, 2),
            " sl=", DoubleToString(sendSl, digits),
            " tp=", DoubleToString(sendTp, digits));
      SendLine("ERROR|" + g_terminalId + "|" + symbol + "|OrderFailed|" + IntegerToString((long)openRetcode));
      return;
   }

   ulong ticket = g_trade.ResultOrder();
   if (ticket == 0)
   {
      // Async mode: request was sent, but broker-side ticket may not be available yet.
      // Do not block here; try a fast, non-blocking resolve by symbol.
      if (PositionSelect(symbol))
      {
         long posTicket = PositionGetInteger(POSITION_TICKET);
         if (posTicket > 0)
         {
            ticket = (ulong)posTicket;
         }
      }

      if (ticket == 0)
      {
         if (VerboseLogs)
         {
            Print("Async order accepted, ticket pending. retcode=", g_trade.ResultRetcode(), " ",
                  g_trade.ResultRetcodeDescription(),
                  " symbol=", symbol, " side=", side);
         }
         return;
      }
   }

   bool sltpOk = WatchdogModify(ticket, symbol, sendSl, sendTp);
   if (!sltpOk)
   {
      Print("Watchdog warning: SL/TP may not be fully applied. ticket=", ticket,
            " symbol=", symbol,
            " sl=", DoubleToString(sendSl, DigitsForSymbol(symbol)),
            " tp=", DoubleToString(sendTp, DigitsForSymbol(symbol)));
   }

   ulong finalTicket = ticket;
   double fillPrice = 0.0;
   if (SelectPositionByTicketOrSymbol(ticket, symbol))
   {
      long posTicket = PositionGetInteger(POSITION_TICKET);
      if (posTicket > 0) finalTicket = (ulong)posTicket;
      fillPrice = PositionGetDouble(POSITION_PRICE_OPEN);
   }

   SendLine("FILLED|" + g_terminalId + "|" + symbol + "|" + side + "|" +
            DoubleToString(lots, 2) + "|" +
            DoubleToString(fillPrice, digits) + "|" +
            DoubleToString(sendSl, digits) + "|" +
            DoubleToString(sendTp, digits) + "|" +
            IntegerToString(finalTicket));

   if (finalTicket > 0)
   {
      g_lastReportedTicket = finalTicket;
   }
}

bool WatchdogModify(ulong ticket, string symbol, double slReq, double tpReq)
{
   if (ticket == 0) return false;
   if (slReq <= 0.0 && tpReq <= 0.0) return true;

   AddPendingModification(ticket, symbol, slReq, tpReq);
   ProcessPendingModifications();
   return !HasPendingModification(ticket);
}

void AddPendingModification(ulong ticket, string symbol, double expectedSl, double expectedTp)
{
   if (ticket == 0) return;
   if (expectedSl <= 0.0 && expectedTp <= 0.0) return;

   int digits = DigitsForSymbol(symbol);
   double normalizedSl = expectedSl > 0.0 ? NormalizeDouble(expectedSl, digits) : 0.0;
   double normalizedTp = expectedTp > 0.0 ? NormalizeDouble(expectedTp, digits) : 0.0;

   for (int i = ArraySize(g_pendingModifications) - 1; i >= 0; i--)
   {
      if (g_pendingModifications[i].Ticket == ticket)
      {
         g_pendingModifications[i].Symbol = symbol;
         g_pendingModifications[i].ExpectedSl = normalizedSl;
         g_pendingModifications[i].ExpectedTp = normalizedTp;
         g_pendingModifications[i].Attempts = 0;
         g_pendingModifications[i].LastAttemptMs = 0;
         return;
      }
   }

   int next = ArraySize(g_pendingModifications);
   ArrayResize(g_pendingModifications, next + 1);
   g_pendingModifications[next].Ticket = ticket;
   g_pendingModifications[next].Symbol = symbol;
   g_pendingModifications[next].ExpectedSl = normalizedSl;
   g_pendingModifications[next].ExpectedTp = normalizedTp;
    g_pendingModifications[next].Attempts = 0;
    g_pendingModifications[next].LastAttemptMs = 0;
}

bool HasPendingModification(ulong ticket)
{
   for (int i = ArraySize(g_pendingModifications) - 1; i >= 0; i--)
   {
      if (g_pendingModifications[i].Ticket == ticket)
      {
         return true;
      }
   }

   return false;
}

void ConfigureTradeExecution(string symbol)
{
   // Enforce strict execution policy on every order send (protect against runtime overrides).
   int deviation = MaxSlippagePoints > 0 ? MaxSlippagePoints : 10;
   g_trade.SetDeviationInPoints(deviation);

   ENUM_ORDER_TYPE_FILLING preferred = PreferFokFilling ? ORDER_FILLING_FOK : ORDER_FILLING_IOC;
   if (IsFillingSupported(symbol, preferred))
   {
      g_trade.SetTypeFilling(preferred);
      return;
   }

   if (IsFillingSupported(symbol, ORDER_FILLING_IOC))
   {
      g_trade.SetTypeFilling(ORDER_FILLING_IOC);
      return;
   }

   if (IsFillingSupported(symbol, ORDER_FILLING_FOK))
   {
      g_trade.SetTypeFilling(ORDER_FILLING_FOK);
      return;
   }

   g_trade.SetTypeFilling(ORDER_FILLING_RETURN);
}

bool IsFillingSupported(string symbol, ENUM_ORDER_TYPE_FILLING filling)
{
   long mode = 0;
   if (!SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE, mode))
   {
      return false;
   }

   if (filling == ORDER_FILLING_FOK)
   {
      return (mode & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK;
   }

   if (filling == ORDER_FILLING_IOC)
   {
      return (mode & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC;
   }

   return true;
}

void RemovePendingModificationAt(int index)
{
   int count = ArraySize(g_pendingModifications);
   if (index < 0 || index >= count) return;

   int last = count - 1;
   if (index != last)
   {
      g_pendingModifications[index].Ticket = g_pendingModifications[last].Ticket;
      g_pendingModifications[index].Symbol = g_pendingModifications[last].Symbol;
      g_pendingModifications[index].ExpectedSl = g_pendingModifications[last].ExpectedSl;
      g_pendingModifications[index].ExpectedTp = g_pendingModifications[last].ExpectedTp;
      g_pendingModifications[index].Attempts = g_pendingModifications[last].Attempts;
      g_pendingModifications[index].LastAttemptMs = g_pendingModifications[last].LastAttemptMs;
   }

   ArrayResize(g_pendingModifications, last);
}

double ModificationTolerance(string symbol)
{
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   if (point <= 0.0) point = _Point;

   double tolerance = point * 1.5;
   if (tolerance < 0.00001) tolerance = 0.00001;
   return tolerance;
}

bool StopMatches(double current, double expected, double tolerance)
{
   if (expected <= 0.0)
   {
      return MathAbs(current) <= tolerance;
   }

   return MathAbs(current - expected) <= tolerance;
}

double MinStopDistance(string symbol)
{
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   if (point <= 0.0) point = _Point;

   long stopsLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   long freezeLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_FREEZE_LEVEL);
   double minDistance = MathMax((double)stopsLevel, (double)freezeLevel) * point;
   if (minDistance < point) minDistance = point;
   return minDistance;
}

bool NormalizeStopsForPosition(string symbol, ENUM_POSITION_TYPE side, double &slPrice, double &tpPrice)
{
   int digits = DigitsForSymbol(symbol);
   if (slPrice > 0.0) slPrice = NormalizeDouble(slPrice, digits);
   if (tpPrice > 0.0) tpPrice = NormalizeDouble(tpPrice, digits);

   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   if (bid <= 0.0 || ask <= 0.0) return false;

   double minDistance = MinStopDistance(symbol);
   if (side == POSITION_TYPE_BUY)
   {
      if (slPrice > 0.0)
      {
         double maxAllowedSl = NormalizeDouble(bid - minDistance, digits);
         if (slPrice > maxAllowedSl) slPrice = maxAllowedSl;
         if (slPrice <= 0.0) slPrice = 0.0;
      }

      if (tpPrice > 0.0)
      {
         double minAllowedTp = NormalizeDouble(ask + minDistance, digits);
         if (tpPrice < minAllowedTp) tpPrice = minAllowedTp;
      }
      return true;
   }

   if (side == POSITION_TYPE_SELL)
   {
      if (slPrice > 0.0)
      {
         double minAllowedSl = NormalizeDouble(ask + minDistance, digits);
         if (slPrice < minAllowedSl) slPrice = minAllowedSl;
      }

      if (tpPrice > 0.0)
      {
         double maxAllowedTp = NormalizeDouble(bid - minDistance, digits);
         if (tpPrice > maxAllowedTp) tpPrice = maxAllowedTp;
         if (tpPrice <= 0.0) tpPrice = 0.0;
      }
      return true;
   }

   return false;
}

bool NormalizeStopsForTradeSide(string symbol, string side, double &slPrice, double &tpPrice)
{
   string sideNorm = NormalizeText(side);
   if (sideNorm == "BUY")
   {
      return NormalizeStopsForPosition(symbol, POSITION_TYPE_BUY, slPrice, tpPrice);
   }

   if (sideNorm == "SELL")
   {
      return NormalizeStopsForPosition(symbol, POSITION_TYPE_SELL, slPrice, tpPrice);
   }

   return false;
}

void ProcessPendingModifications()
{
   for (int i = ArraySize(g_pendingModifications) - 1; i >= 0; i--)
   {
      ulong ticket = g_pendingModifications[i].Ticket;
      string symbol = g_pendingModifications[i].Symbol;
      double expectedSl = g_pendingModifications[i].ExpectedSl;
      double expectedTp = g_pendingModifications[i].ExpectedTp;
      int attempts = g_pendingModifications[i].Attempts;
      ulong lastAttemptMs = g_pendingModifications[i].LastAttemptMs;
      ulong nowMs = (ulong)GetTickCount();

      if (ticket == 0)
      {
         RemovePendingModificationAt(i);
         continue;
      }

      if (!PositionSelectByTicket(ticket))
      {
         // Position is closed or no longer available -> cleanup.
         RemovePendingModificationAt(i);
         continue;
      }

      ENUM_POSITION_TYPE side = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      if (!NormalizeStopsForPosition(symbol, side, expectedSl, expectedTp))
      {
         continue;
      }

      double currentSl = PositionGetDouble(POSITION_SL);
      double currentTp = PositionGetDouble(POSITION_TP);
      double tolerance = ModificationTolerance(symbol);

      bool slOk = StopMatches(currentSl, expectedSl, tolerance);
      bool tpOk = StopMatches(currentTp, expectedTp, tolerance);
      if (slOk && tpOk)
      {
         RemovePendingModificationAt(i);
         continue;
      }

      if (attempts >= MAX_MODIFY_RETRIES)
      {
         if (VerboseLogs)
         {
            int digits = DigitsForSymbol(symbol);
            Print("Persistent Watchdog give up after retries: ticket=", ticket,
                  " symbol=", symbol,
                  " expectedSL=", DoubleToString(expectedSl, digits),
                  " expectedTP=", DoubleToString(expectedTp, digits));
         }
         RemovePendingModificationAt(i);
         continue;
      }

      if (lastAttemptMs > 0 && (nowMs - lastAttemptMs) < MODIFY_RETRY_INTERVAL_MS)
      {
         continue;
      }

      g_pendingModifications[i].ExpectedSl = expectedSl;
      g_pendingModifications[i].ExpectedTp = expectedTp;
      g_pendingModifications[i].Attempts = attempts + 1;
      g_pendingModifications[i].LastAttemptMs = nowMs;

      bool modified = g_trade.PositionModify(ticket, expectedSl, expectedTp);
      if (!modified)
      {
         if (VerboseLogs)
         {
            int digits = DigitsForSymbol(symbol);
            Print("Persistent Watchdog modify failed: retcode=", g_trade.ResultRetcode(), " ",
                  g_trade.ResultRetcodeDescription(),
                  " ticket=", ticket,
                  " symbol=", symbol,
                  " expectedSL=", DoubleToString(expectedSl, digits),
                  " expectedTP=", DoubleToString(expectedTp, digits));
         }
         continue;
      }

      // Verify right away after successful modify call.
      if (PositionSelectByTicket(ticket))
      {
         currentSl = PositionGetDouble(POSITION_SL);
         currentTp = PositionGetDouble(POSITION_TP);
         slOk = StopMatches(currentSl, expectedSl, tolerance);
         tpOk = StopMatches(currentTp, expectedTp, tolerance);
         if (slOk && tpOk)
         {
            RemovePendingModificationAt(i);
         }
      }
   }
}

bool BuildStopPrices(string symbol, string side, double slDistance, double tpDistance, double &slPrice, double &tpPrice)
{
   slPrice = 0.0;
   tpPrice = 0.0;

   int digits = DigitsForSymbol(symbol);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   if (point <= 0.0) point = _Point;

   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   if (bid <= 0.0 || ask <= 0.0) return false;

   long stopsLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   long freezeLevel = SymbolInfoInteger(symbol, SYMBOL_TRADE_FREEZE_LEVEL);
   double minDistance = MathMax((double)stopsLevel, (double)freezeLevel) * point;
   if (minDistance < point) minDistance = point;

   string sideNorm = NormalizeText(side);
   if (sideNorm == "BUY")
   {
      if (slDistance > 0.0)
      {
         slPrice = NormalizeDouble(bid - slDistance, digits);
         double maxAllowedSl = NormalizeDouble(bid - minDistance, digits);
         if (slPrice > maxAllowedSl) slPrice = maxAllowedSl;
         if (slPrice <= 0.0) slPrice = 0.0;
      }

      if (tpDistance > 0.0)
      {
         tpPrice = NormalizeDouble(ask + tpDistance, digits);
         double minAllowedTp = NormalizeDouble(ask + minDistance, digits);
         if (tpPrice < minAllowedTp) tpPrice = minAllowedTp;
      }

      return true;
   }

   if (sideNorm == "SELL")
   {
      if (slDistance > 0.0)
      {
         slPrice = NormalizeDouble(ask + slDistance, digits);
         double minAllowedSl = NormalizeDouble(ask + minDistance, digits);
         if (slPrice < minAllowedSl) slPrice = minAllowedSl;
      }

      if (tpDistance > 0.0)
      {
         tpPrice = NormalizeDouble(bid - tpDistance, digits);
         double maxAllowedTp = NormalizeDouble(bid - minDistance, digits);
         if (tpPrice > maxAllowedTp) tpPrice = maxAllowedTp;
         if (tpPrice <= 0.0) tpPrice = 0.0;
      }

      return true;
   }

   return false;
}

bool SelectPositionByTicketOrSymbol(ulong ticket, string symbol)
{
   if (ticket > 0 && PositionSelectByTicket(ticket))
   {
      return true;
   }

   return PositionSelect(symbol);
}

int DigitsForSymbol(string symbol)
{
   long digits = SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   if (digits < 0) digits = _Digits;
   return (int)digits;
}

string NormalizeText(string value)
{
   StringTrimLeft(value);
   StringTrimRight(value);
   StringToUpper(value);
   return value;
}

bool SymbolsEquivalent(string left, string right)
{
   string a = NormalizeText(left);
   string b = NormalizeText(right);
   if (a == "" || b == "") return false;
   if (a == b) return true;
   return StringFind(a, b) == 0 || StringFind(b, a) == 0;
}

void ClosePositionsBySymbol(string symbol)
{
   string targetSymbol = NormalizeText(symbol);
   if (targetSymbol == "")
   {
      CloseAllPositions();
      return;
   }

   for (int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if (ticket == 0)
      {
         continue;
      }

      if (!PositionSelectByTicket(ticket))
      {
         continue;
      }

      string posSymbol = PositionGetString(POSITION_SYMBOL);
      if (!SymbolsEquivalent(posSymbol, targetSymbol))
      {
         continue;
      }

      g_trade.PositionClose(ticket);
   }
}

void CloseAllPositions()
{
   for (int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if (ticket > 0)
      {
         g_trade.PositionClose(ticket);
      }
   }
}

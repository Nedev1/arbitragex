#property strict
#property description "Bridge (MT4 Named Pipes - Production)"

#define GENERIC_READ -2147483648
#define GENERIC_WRITE 0x40000000
#define OPEN_EXISTING 3
#define PIPE_READMODE_MESSAGE 0x00000002
#define INVALID_PIPE_HANDLE -1

#define ERR_SERVER_BUSY 4
#define ERR_OFF_QUOTES 136
#define ERR_BROKER_BUSY 137
#define ERR_REQUOTE 138
#define ERR_TRADE_CONTEXT_BUSY 146
#define ERR_PRICE_CHANGED 135

#import "kernel32.dll"
int CreateFileW(string name, int access, int share, int security, int creation, int flags, int templateHandle);
bool ReadFile(int handle, uchar& buffer[], int bytesToRead, int& bytesRead, int overlapped);
bool WriteFile(int handle, uchar& buffer[], int bytesToWrite, int& bytesWritten, int overlapped);
bool FlushFileBuffers(int handle);
bool CloseHandle(int handle);
bool WaitNamedPipeW(string name, int timeout);
bool SetNamedPipeHandleState(int handle, int& mode, int maxcoll, int maxtime);
bool PeekNamedPipe(int handle, uchar& buffer[], int buflen, int& bytesRead, int& bytesAvail, int& bytesLeft);
bool QueryPerformanceCounter(long& value);
bool QueryPerformanceFrequency(long& value);
#import

input string TerminalId = "MT4-01";
input string PipeName = "\\\\.\\pipe\\TradePipe";
input string SymbolSuffix = "";
input bool VerboseLogs = false;
input int SlippagePoints = 30;
input int MaxSlippagePoints = 10;
input int MaxSendRetries = 3;
input int MaxModifyRetries = 6;
input int DefaultMagic = 910001;
input string TradeCommentPrefix = "";

int g_pipe = INVALID_PIPE_HANDLE;
string g_terminalId = "";
string g_pendingIn = "";
long g_qpcFrequency = 0;

string I64ToString(long value)
{
   return StringFormat("%I64d", value);
}

int OnInit()
{
   g_terminalId = TerminalId;
   InitHighResolutionClock();
   EventSetTimer(1);

   // Try immediate connect, but do not fail EA if the hub isn't running yet.
   ConnectPipe();
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   DisconnectPipe();
}

void OnTick()
{
   static int prevOpenOrders = -1;
   int currentOpenOrders = CountOpenMarketOrders();

   if (prevOpenOrders >= 0 && prevOpenOrders > 0 && currentOpenOrders == 0 && IsPipeConnected())
   {
      SendLine("CLOSED|" + g_terminalId);
   }

   if (IsPipeConnected())
   {
      double bid = MarketInfo(Symbol(), MODE_BID);
      double ask = MarketInfo(Symbol(), MODE_ASK);
      long ts = NowMicros();
      string msg = "TICK|" + g_terminalId + "|" + Symbol() + "|" + DoubleToString(bid, 5) + "|" + DoubleToString(ask, 5) + "|" + I64ToString(ts);
      SendLine(msg);
   }

   prevOpenOrders = currentOpenOrders;
}

void OnTimer()
{
   if (!IsPipeConnected())
   {
      ConnectPipe();
      return;
   }

   // Drain multiple commands per timer pulse when available.
   for (int i = 0; i < 64; i++)
   {
      string cmd = ReadNextLine();
      if (cmd == "") break;
      ProcessCommand(cmd);
   }
}

void InitHighResolutionClock()
{
   long freq = 0;
   if (QueryPerformanceFrequency(freq) && freq > 0)
   {
      g_qpcFrequency = freq;
   }
}

long NowMicros()
{
   if (g_qpcFrequency > 0)
   {
      long counter = 0;
      if (QueryPerformanceCounter(counter))
      {
         return (long)MathRound((double)counter * 1000000.0 / (double)g_qpcFrequency);
      }
   }

   return (long)GetTickCount() * 1000;
}

bool IsPipeConnected()
{
   return g_pipe != INVALID_PIPE_HANDLE;
}

bool ConnectPipe()
{
   if (IsPipeConnected()) return true;

   if (!WaitNamedPipeW(PipeName, 2000))
   {
      return false;
   }

   g_pipe = CreateFileW(PipeName, GENERIC_READ | GENERIC_WRITE, 0, 0, OPEN_EXISTING, 0, 0);
   if (g_pipe == INVALID_PIPE_HANDLE)
   {
      return false;
   }

   int mode = PIPE_READMODE_MESSAGE;
   SetNamedPipeHandleState(g_pipe, mode, 0, 0);
   g_pendingIn = "";

   if (VerboseLogs) Print("Connected to Pipe");
   SendLine("HELLO|" + g_terminalId);
   return true;
}

void DisconnectPipe()
{
   if (g_pipe != INVALID_PIPE_HANDLE)
   {
      CloseHandle(g_pipe);
      g_pipe = INVALID_PIPE_HANDLE;
   }
   g_pendingIn = "";
}

bool SendLine(string line)
{
   if (!IsPipeConnected()) return false;

   string payload = line + "\n";
   uchar data[];
   StringToCharArray(payload, data);
   int len = ArraySize(data) - 1; // remove null terminator
   if (len <= 0) return false;

   int written = 0;
   if (VerboseLogs) Print("WriteFile bytes=", len);

   bool ok = WriteFile(g_pipe, data, len, written, 0);
   if (!ok)
   {
      if (VerboseLogs) Print("WriteFile failed, disconnecting");
      if (g_pipe != INVALID_PIPE_HANDLE)
      {
         CloseHandle(g_pipe);
         g_pipe = INVALID_PIPE_HANDLE;
      }
      g_pendingIn = "";
      return false;
   }

   // Flush only in verbose mode to avoid per-tick overhead.
   if (VerboseLogs) FlushFileBuffers(g_pipe);
   return true;
}

string ReadNextLine()
{
   if (!IsPipeConnected()) return "";

   int idx = StringFind(g_pendingIn, "\n");
   if (idx >= 0)
   {
      string line = StringSubstr(g_pendingIn, 0, idx);
      g_pendingIn = StringSubstr(g_pendingIn, idx + 1);
      StringTrimLeft(line);
      StringTrimRight(line);
      return line;
   }

   uchar peek[];
   ArrayResize(peek, 1);
   int bytesRead = 0;
   int bytesAvail = 0;
   int bytesLeft = 0;
   if (!PeekNamedPipe(g_pipe, peek, 0, bytesRead, bytesAvail, bytesLeft))
   {
      if (VerboseLogs) Print("PeekNamedPipe failed, disconnecting");
      if (g_pipe != INVALID_PIPE_HANDLE)
      {
         CloseHandle(g_pipe);
         g_pipe = INVALID_PIPE_HANDLE;
      }
      g_pendingIn = "";
      return "";
   }

   if (bytesAvail <= 0) return "";

   int readSize = bytesAvail;
   if (readSize < 1) readSize = 1;
   if (readSize > 2048) readSize = 2048;

   uchar buffer[];
   ArrayResize(buffer, readSize);
   int read = 0;
   bool ok = ReadFile(g_pipe, buffer, readSize, read, 0);
   if (!ok || read <= 0)
   {
      if (VerboseLogs) Print("ReadFile failed, disconnecting");
      if (g_pipe != INVALID_PIPE_HANDLE)
      {
         CloseHandle(g_pipe);
         g_pipe = INVALID_PIPE_HANDLE;
      }
      g_pendingIn = "";
      return "";
   }

   string chunk = CharArrayToString(buffer, 0, read);
   g_pendingIn += chunk;

   idx = StringFind(g_pendingIn, "\n");
   if (idx < 0)
   {
      // Protect against malformed giant lines.
      if (StringLen(g_pendingIn) > 8192)
      {
         g_pendingIn = "";
      }
      return "";
   }

   string outLine = StringSubstr(g_pendingIn, 0, idx);
   g_pendingIn = StringSubstr(g_pendingIn, idx + 1);
   StringTrimLeft(outLine);
   StringTrimRight(outLine);
   return outLine;
}

void ProcessCommand(string cmd)
{
   if (cmd == "") return;

   if (StringFind(cmd, "KILL|") == 0)
   {
      CloseAllPositions();
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

   string terminal = "";
   string symbolRaw = "";
   string side = "";
   double lots = 0.0;
   double slDistance = 0.0;
   double tpDistance = 0.0;
   int magic = DefaultMagic;

   if (!ParseTradeCommand(cmd, terminal, symbolRaw, side, lots, slDistance, tpDistance, magic))
   {
      return;
   }

   if (NormalizeText(terminal) != NormalizeText(g_terminalId))
   {
      return;
   }

   string symbol = ResolveTradeSymbol(symbolRaw);
   if (symbol == "")
   {
      Print("Unknown symbol in command: ", symbolRaw);
      return;
   }

   if (lots <= 0.0)
   {
      return;
   }

   double slPrice = 0.0;
   double tpPrice = 0.0;
   if (!BuildStopPrices(symbol, side, slDistance, tpDistance, slPrice, tpPrice))
   {
      Print("BuildStopPrices failed. symbol=", symbol, " side=", side,
            " slDistance=", DoubleToString(slDistance, 5),
            " tpDistance=", DoubleToString(tpDistance, 5));
      return;
   }

   int ticket = -1;
   if (!OpenMarketOrder(symbol, side, lots, magic, slPrice, tpPrice, ticket))
   {
      return;
   }

   bool sltpOk = WatchdogModify(ticket, symbol, slPrice, tpPrice);
   if (!sltpOk)
   {
      Print("Watchdog warning: SL/TP may not be fully applied. ticket=", ticket,
            " symbol=", symbol,
            " sl=", DoubleToString(slPrice, SymbolDigits(symbol)),
            " tp=", DoubleToString(tpPrice, SymbolDigits(symbol)));
   }

   double finalLots = lots;
   double finalSl = slPrice;
   double finalTp = tpPrice;
   double fillPrice = 0.0;
   if (OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
   {
      finalLots = OrderLots();
      fillPrice = OrderOpenPrice();
      if (OrderStopLoss() > 0.0) finalSl = OrderStopLoss();
      if (OrderTakeProfit() > 0.0) finalTp = OrderTakeProfit();
   }

   int digits = SymbolDigits(symbol);
   SendLine("FILLED|" + g_terminalId + "|" + symbol + "|" + NormalizeText(side) + "|" +
            DoubleToString(finalLots, 2) + "|" +
            DoubleToString(fillPrice, digits) + "|" +
            DoubleToString(finalSl, digits) + "|" +
            DoubleToString(finalTp, digits) + "|" +
            IntegerToString(ticket));
}

bool ParseTradeCommand(string cmd, string& terminal, string& symbol, string& side, double& lots, double& slDistance, double& tpDistance, int& magic)
{
   string parts[];
   int n = StringSplit(cmd, '|', parts);
   if (n < 7) return false;

   if (NormalizeText(parts[0]) == "TRADE")
   {
      // TRADE|terminal|symbol|side|lots|sl|tp[|magic]
      if (n < 7) return false;
      terminal = parts[1];
      symbol = parts[2];
      side = parts[3];
      lots = StrToDouble(parts[4]);
      slDistance = MathAbs(StrToDouble(parts[5]));
      tpDistance = MathAbs(StrToDouble(parts[6]));
      if (n >= 8) magic = (int)StrToInteger(parts[7]);
      return true;
   }

   // terminal|symbol|side|lots|sl|tp|magic
   terminal = parts[0];
   symbol = parts[1];
   side = parts[2];
   lots = StrToDouble(parts[3]);
   slDistance = MathAbs(StrToDouble(parts[4]));
   tpDistance = MathAbs(StrToDouble(parts[5]));
   magic = (int)StrToInteger(parts[6]);
   return true;
}

string ResolveTradeSymbol(string requested)
{
   string sym = requested;
   StringTrimLeft(sym);
   StringTrimRight(sym);
   if (sym == "") return "";

   if (EnsureSymbolAvailable(sym))
   {
      return sym;
   }

   if (SymbolSuffix != "")
   {
      string withSuffix = sym + SymbolSuffix;
      if (EnsureSymbolAvailable(withSuffix))
      {
         return withSuffix;
      }
   }

   return "";
}

bool EnsureSymbolAvailable(string symbol)
{
   if (symbol == "") return false;
   if (StringCompare(Symbol(), symbol, false) == 0) return true;

   SymbolSelect(symbol, true);
   ResetLastError();
   double p = MarketInfo(symbol, MODE_BID);
   int err = GetLastError();
   if (err == 0) return true;

   // Some brokers return 0 bid but still no error for inactive symbols.
   if (p >= 0 && err == 0) return true;
   return false;
}

int EffectiveSlippagePoints()
{
   if (MaxSlippagePoints > 0) return MaxSlippagePoints;
   if (SlippagePoints > 0) return SlippagePoints;
   return 10;
}

bool OpenMarketOrder(string symbol, string side, double lots, int magic, double slPrice, double tpPrice, int& ticketOut)
{
   string sideNorm = NormalizeText(side);
   int type = -1;
   if (sideNorm == "BUY") type = OP_BUY;
   if (sideNorm == "SELL") type = OP_SELL;
   if (type < 0) return false;

   int retries = MathMax(1, MaxSendRetries);
   int digits = SymbolDigits(symbol);
   int slippage = EffectiveSlippagePoints();
   double slTarget = slPrice > 0.0 ? NormalizeDouble(slPrice, digits) : 0.0;
   double tpTarget = tpPrice > 0.0 ? NormalizeDouble(tpPrice, digits) : 0.0;

   for (int attempt = 0; attempt < retries; attempt++)
   {
      RefreshRates();
      double price = (type == OP_BUY) ? MarketInfo(symbol, MODE_ASK) : MarketInfo(symbol, MODE_BID);
      price = NormalizeDouble(price, digits);

      // Inject SL/TP on initial execution for faster protection.
      int ticket = OrderSend(symbol, type, lots, price, slippage, slTarget, tpTarget, TradeCommentPrefix, magic, 0, clrDodgerBlue);
      if (ticket > 0)
      {
         ticketOut = ticket;
         return true;
      }

      int err = GetLastError();
      if (VerboseLogs || !IsRetryableTradeError(err))
      {
         Print("OrderSend failed: err=", err, " symbol=", symbol, " side=", sideNorm, " attempt=", attempt + 1, "/", retries);
      }

      if (!IsRetryableTradeError(err))
      {
         break;
      }

      Sleep(60);
      ResetLastError();
   }

   return false;
}

bool IsRetryableTradeError(int err)
{
   return err == ERR_REQUOTE ||
          err == ERR_PRICE_CHANGED ||
          err == ERR_OFF_QUOTES ||
          err == ERR_BROKER_BUSY ||
          err == ERR_TRADE_CONTEXT_BUSY ||
          err == ERR_SERVER_BUSY;
}

bool WatchdogModify(int ticket, string symbol, double slReq, double tpReq)
{
   if (slReq <= 0.0 && tpReq <= 0.0) return true;

   double point = MarketInfo(symbol, MODE_POINT);
   if (point <= 0.0) point = Point;
   int digits = SymbolDigits(symbol);
   double slTarget = slReq > 0.0 ? NormalizeDouble(slReq, digits) : 0.0;
   double tpTarget = tpReq > 0.0 ? NormalizeDouble(tpReq, digits) : 0.0;
   int retries = MathMax(1, MaxModifyRetries);

   for (int i = 0; i < retries; i++)
   {
      if (!OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
      {
         Sleep(20);
         continue;
      }

      double sl = OrderStopLoss();
      double tp = OrderTakeProfit();

      bool slOk = (slTarget <= 0.0) || (MathAbs(sl - slTarget) <= point * 1.5);
      bool tpOk = (tpTarget <= 0.0) || (MathAbs(tp - tpTarget) <= point * 1.5);
      if (slOk && tpOk) return true;

      bool modified = OrderModify(ticket, OrderOpenPrice(), slTarget, tpTarget, 0, clrNONE);
      if (!modified)
      {
         int err = GetLastError();
         if (VerboseLogs || !IsRetryableTradeError(err))
         {
            Print("OrderModify failed: err=", err, " ticket=", ticket, " sl=", DoubleToString(slTarget, digits), " tp=", DoubleToString(tpTarget, digits));
         }
      }

      Sleep(40);
   }

   return false;
}

bool BuildStopPrices(string symbol, string side, double slDistance, double tpDistance, double& slPrice, double& tpPrice)
{
   slPrice = 0.0;
   tpPrice = 0.0;

   int digits = SymbolDigits(symbol);
   double point = MarketInfo(symbol, MODE_POINT);
   if (point <= 0.0) point = Point;

   double bid = MarketInfo(symbol, MODE_BID);
   double ask = MarketInfo(symbol, MODE_ASK);
   if (bid <= 0.0 || ask <= 0.0) return false;

   int stopsLevel = (int)MarketInfo(symbol, MODE_STOPLEVEL);
   int freezeLevel = (int)MarketInfo(symbol, MODE_FREEZELEVEL);
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

int CountOpenMarketOrders()
{
   int count = 0;
   int total = OrdersTotal();
   for (int i = total - 1; i >= 0; i--)
   {
      if (!OrderSelect(i, SELECT_BY_POS, MODE_TRADES)) continue;
      int type = OrderType();
      if (type == OP_BUY || type == OP_SELL)
      {
         count++;
      }
   }
   return count;
}

void CloseAllPositions()
{
   int total = OrdersTotal();
   for (int i = total - 1; i >= 0; i--)
   {
      if (!OrderSelect(i, SELECT_BY_POS, MODE_TRADES)) continue;

      int type = OrderType();
      if (type != OP_BUY && type != OP_SELL) continue;

      int ticket = OrderTicket();
      string symbol = OrderSymbol();
      double lots = OrderLots();

      int retries = MathMax(1, MaxSendRetries);
      for (int attempt = 0; attempt < retries; attempt++)
      {
         RefreshRates();
         double price = (type == OP_BUY) ? MarketInfo(symbol, MODE_BID) : MarketInfo(symbol, MODE_ASK);
         bool ok = OrderClose(ticket, lots, price, SlippagePoints, clrRed);
         if (ok) break;

         int err = GetLastError();
         if (!IsRetryableTradeError(err))
         {
            if (VerboseLogs) Print("OrderClose failed: err=", err, " ticket=", ticket);
            break;
         }
         Sleep(60);
         ResetLastError();
      }
   }
}

int SymbolDigits(string symbol)
{
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);
   if (digits < 0) digits = Digits;
   return digits;
}

string NormalizeText(string value)
{
   StringTrimLeft(value);
   StringTrimRight(value);
   StringToUpper(value);
   return value;
}

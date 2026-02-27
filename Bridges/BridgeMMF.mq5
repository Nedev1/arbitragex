#property strict
#property description "Nexus HFT Bridge MMF (ArbitrageX 2.0 layout)"

#include <Trade/Trade.mqh>

#define PAGE_READWRITE 0x04
#define FILE_MAP_ALL_ACCESS 0x000f001f
#define INVALID_HANDLE_VALUE -1

#define SHARED_BLOCK_BYTES 256
#define OFFSET_MASTER_TICK 8
#define OFFSET_SIGNAL 64
#define OFFSET_REPORT 128
#define OFFSET_SLAVE_TICK 192

#define OFFSET_SIGNAL_CMD (OFFSET_SIGNAL + 8)
#define OFFSET_REPORT_STATUS (OFFSET_REPORT + 8)

#define TICK_DATA_BYTES 56
#define TRADE_SIGNAL_BYTES 64
#define EXEC_REPORT_BYTES 64

#define CMD_NONE 0
#define CMD_OPEN 1
#define CMD_BUY 1
#define CMD_KILL 2

#define STATUS_NONE 0
#define STATUS_ACCEPTED 1
#define STATUS_FILLED 2
#define STATUS_REJECTED 3
#define STATUS_FLAT 4

#define STRICT_DEVIATION_POINTS 8

input string MmfName = "Local\\Nexus_MMF_v1";
input bool VerboseLogs = false;

long g_map = 0;
long g_view = 0;
ulong g_lastSignalSeq = 0;
ulong g_reportSeq = 0;
ulong g_slaveTickSeq = 0;
string g_symbol = "";

struct TickData
{
   ulong Seq;           // 0
   double Bid;          // 8
   double Ask;          // 16
   ulong TsUs;          // 24
   uint SymbolHash;     // 32
   uint SourceHash;     // 36
   uchar Pad[16];       // 40..55
};

struct TradeSignal
{
   ulong Seq;           // 0
   uint Cmd;            // 8 (1=open, 2=kill)
   uint Side;           // 12 (1=buy, -1=sell via 0xFFFFFFFF)
   double Lots;         // 16
   double Sl;           // 24
   double Tp;           // 32
   double Reference;    // 40 (rolling offset snapshot)
   double SlaveBid;     // 48
   double SlaveAsk;     // 56
};

struct ExecutionReport
{
   ulong Seq;           // 0
   uint Status;         // 8
   uint Side;           // 12
   ulong Ticket;        // 16
   double FillPrice;    // 24
   double FillLots;     // 32
   ulong TsUs;          // 40
   ulong Pad;           // 48
   int ErrorCode;       // 56
   uint Reserved;       // 60
};

#import "kernel32.dll"
long CreateFileMappingW(long hFile, long lpFileMappingAttributes, uint flProtect, uint dwMaximumSizeHigh, uint dwMaximumSizeLow, string lpName);
long OpenFileMappingW(uint dwDesiredAccess, bool bInheritHandle, string lpName);
long MapViewOfFile(long hFileMappingObject, uint dwDesiredAccess, uint dwFileOffsetHigh, uint dwFileOffsetLow, ulong dwNumberOfBytesToMap);
bool UnmapViewOfFile(long lpBaseAddress);
bool CloseHandle(long hObject);

void RtlMoveMemory(TickData &Destination, long Source, int Length);
void RtlMoveMemory(TradeSignal &Destination, long Source, int Length);
void RtlMoveMemory(ExecutionReport &Destination, long Source, int Length);
void RtlMoveMemory(uint &Destination, long Source, int Length);
void RtlMoveMemory(ulong &Destination, long Source, int Length);

void RtlMoveMemory(long Destination, TickData &Source, int Length);
void RtlMoveMemory(long Destination, ExecutionReport &Source, int Length);
void RtlMoveMemory(long Destination, uint &Source, int Length);
void RtlMoveMemory(long Destination, ulong &Source, int Length);
#import

int OnInit()
{
   g_symbol = _Symbol;

   if (sizeof(TickData) != TICK_DATA_BYTES || sizeof(TradeSignal) != TRADE_SIGNAL_BYTES || sizeof(ExecutionReport) != EXEC_REPORT_BYTES)
   {
      Print("[BridgeMMF] Layout mismatch: Tick=", sizeof(TickData), " Signal=", sizeof(TradeSignal), " Report=", sizeof(ExecutionReport));
      return INIT_FAILED;
   }

   if (!OpenSharedMemory())
   {
      Print("[BridgeMMF] Failed to map shared memory: ", MmfName);
      return INIT_FAILED;
   }

   // Continue sequences from current MMF state to keep monotonic notifications.
   g_lastSignalSeq = ReadU64(OFFSET_SIGNAL);
   g_reportSeq = ReadU64(OFFSET_REPORT);
   g_slaveTickSeq = ReadU64(OFFSET_SLAVE_TICK);

   EventSetMillisecondTimer(1);
   Print("[BridgeMMF] Initialized for symbol=", g_symbol, " map=", MmfName);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   CloseSharedMemory();
}

void OnTick()
{
   PublishSlaveTick();
   PumpSignals();
}

void OnTimer()
{
   PumpSignals();
}

void OnTradeTransaction(const MqlTradeTransaction &trans, const MqlTradeRequest &request, const MqlTradeResult &result)
{
   if (g_view == 0)
   {
      return;
   }

   if (trans.symbol != g_symbol || trans.type != TRADE_TRANSACTION_DEAL_ADD || trans.deal <= 0)
   {
      return;
   }

   if (!HistoryDealSelect(trans.deal))
   {
      return;
   }

   long entryType = HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
   if (entryType == DEAL_ENTRY_OUT)
   {
      long dealType = HistoryDealGetInteger(trans.deal, DEAL_TYPE);
      int side = 0;
      if (dealType == DEAL_TYPE_BUY)
      {
         side = 1;
      }
      else if (dealType == DEAL_TYPE_SELL)
      {
         side = -1;
      }

      double fillPrice = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
      if (fillPrice <= 0.0)
      {
         fillPrice = trans.price;
      }

      double fillLots = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      if (fillLots <= 0.0)
      {
         fillLots = trans.volume;
      }

      ulong ticket = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
      EmitFlat(side, ticket, fillPrice, fillLots);
   }
}

bool OpenSharedMemory()
{
   if (g_map != 0 || g_view != 0)
   {
      return true;
   }

   g_map = OpenFileMappingW(FILE_MAP_ALL_ACCESS, false, MmfName);
   if (g_map == 0)
   {
      g_map = CreateFileMappingW(INVALID_HANDLE_VALUE, 0, PAGE_READWRITE, 0, SHARED_BLOCK_BYTES, MmfName);
      if (g_map == 0)
      {
         return false;
      }
   }

   g_view = MapViewOfFile(g_map, FILE_MAP_ALL_ACCESS, 0, 0, SHARED_BLOCK_BYTES);
   if (g_view == 0)
   {
      CloseHandle(g_map);
      g_map = 0;
      return false;
   }

   return true;
}

void CloseSharedMemory()
{
   if (g_view != 0)
   {
      UnmapViewOfFile(g_view);
      g_view = 0;
   }

   if (g_map != 0)
   {
      CloseHandle(g_map);
      g_map = 0;
   }
}

void PumpSignals()
{
   if (g_view == 0)
   {
      return;
   }

   uint cmd = ReadU32(OFFSET_SIGNAL_CMD);
   if (cmd == CMD_NONE)
   {
      return;
   }

   ulong seqBefore = ReadU64(OFFSET_SIGNAL);
   if (seqBefore == 0 || seqBefore == g_lastSignalSeq)
   {
      return;
   }

   TradeSignal sig;
   ZeroMemory(sig);
   RtlMoveMemory(sig, g_view + OFFSET_SIGNAL, TRADE_SIGNAL_BYTES);

   ulong seqAfter = ReadU64(OFFSET_SIGNAL);
   if (seqAfter == 0 || seqAfter != seqBefore || sig.Seq != seqAfter)
   {
      return;
   }

   g_lastSignalSeq = seqAfter;

   bool handled = false;
   if (cmd == CMD_OPEN || cmd == CMD_BUY)
   {
      handled = HandleOpen(sig);
   }
   else if (cmd == CMD_KILL)
   {
      handled = HandleKill(sig);
   }
   else
   {
      EmitRejected(0, 0, -1001);
      handled = true;
   }

   // Ack command consumption regardless of trade result to avoid replay storms.
   WriteU32(OFFSET_SIGNAL_CMD, CMD_NONE);

   if (VerboseLogs && handled)
   {
      Print("[BridgeMMF] Signal handled cmd=", cmd, " side=", (int)sig.Side, " lots=", DoubleToString(sig.Lots, 2), " seq=", (long)sig.Seq);
   }
}

bool HandleOpen(const TradeSignal &sig)
{
   int side = DecodeSide(sig.Side);
   if (side == 0)
   {
      side = 1;
   }

   double lots = sig.Lots;
   if (lots <= 0.0)
   {
      EmitRejected(side, 0, -1002);
      return false;
   }

   int digits = DigitsForSymbol(g_symbol);
   double sl = sig.Sl;
   double tp = sig.Tp;
   if (sl > 0.0)
   {
      sl = NormalizeDouble(sl, digits);
   }
   if (tp > 0.0)
   {
      tp = NormalizeDouble(tp, digits);
   }

   MqlTick tick;
   if (!SymbolInfoTick(g_symbol, tick))
   {
      EmitRejected(side, 0, -1003);
      return false;
   }

   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);

   req.action = TRADE_ACTION_DEAL;
   req.symbol = g_symbol;
   req.type = (side > 0) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   req.volume = lots;
   req.type_time = ORDER_TIME_GTC;
   req.type_filling = ResolveFillingMode(g_symbol);
   req.deviation = STRICT_DEVIATION_POINTS;
   req.price = (side > 0) ? tick.ask : tick.bid;
   req.sl = sl;
   req.tp = tp;
   req.comment = "NexusMMF";

   bool ok = OrderSend(req, res);
   if (!ok)
   {
      int err = (int)res.retcode;
      if (err == 0)
      {
         err = (int)GetLastError();
      }

      EmitRejected(side, 0, err);
      return false;
   }

   double fillPrice = res.price;
   if (fillPrice <= 0.0)
   {
      fillPrice = req.price;
   }

   ulong ticket = (ulong)res.deal;
   if (ticket == 0)
   {
      ticket = (ulong)res.order;
   }

   ExecutionReport rep;
   ZeroMemory(rep);
   rep.Seq = NextReportSeq();
   rep.Status = STATUS_FILLED;
   rep.Side = EncodeSide(side);
   rep.Ticket = ticket;
   rep.FillPrice = fillPrice;
   rep.FillLots = lots;
   rep.TsUs = (ulong)GetMicrosecondCount();
   rep.ErrorCode = (int)res.retcode;
   WriteExecutionReport(rep);
   return true;
}

bool HandleKill(const TradeSignal &sig)
{
   if (!PositionSelect(g_symbol))
   {
      EmitFlat(0, 0, 0.0, 0.0);
      return true;
   }

   ENUM_POSITION_TYPE posType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   double volume = PositionGetDouble(POSITION_VOLUME);
   ulong positionTicket = (ulong)PositionGetInteger(POSITION_TICKET);

   if (volume <= 0.0)
   {
      EmitFlat(0, positionTicket, 0.0, 0.0);
      return true;
   }

   MqlTick tick;
   if (!SymbolInfoTick(g_symbol, tick))
   {
      EmitRejected(0, positionTicket, -1004);
      return false;
   }

   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);

   req.action = TRADE_ACTION_DEAL;
   req.symbol = g_symbol;
   req.volume = volume;
   req.type = (posType == POSITION_TYPE_BUY) ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   req.type_time = ORDER_TIME_GTC;
   req.type_filling = ResolveFillingMode(g_symbol);
   req.deviation = STRICT_DEVIATION_POINTS;
   req.price = (req.type == ORDER_TYPE_BUY) ? tick.ask : tick.bid;
   req.position = positionTicket;
   req.comment = "NexusMMF-KILL";

   bool ok = OrderSend(req, res);
   if (!ok)
   {
      int err = (int)res.retcode;
      if (err == 0)
      {
         err = (int)GetLastError();
      }

      EmitRejected(0, positionTicket, err);
      return false;
   }

   double closePrice = res.price;
   if (closePrice <= 0.0)
   {
      closePrice = req.price;
   }

   int closeSide = (req.type == ORDER_TYPE_BUY) ? 1 : -1;
   EmitFlat(closeSide, positionTicket, closePrice, volume);
   return true;
}

void PublishSlaveTick()
{
   if (g_view == 0)
   {
      return;
   }

   MqlTick tickNow;
   if (!SymbolInfoTick(g_symbol, tickNow))
   {
      return;
   }

   if (tickNow.bid <= 0.0 || tickNow.ask <= 0.0)
   {
      return;
   }

   TickData tick;
   ZeroMemory(tick);
   tick.Seq = 0; // written last for reader coherence
   tick.Bid = tickNow.bid;
   tick.Ask = tickNow.ask;
   tick.TsUs = (ulong)GetMicrosecondCount();
   tick.SymbolHash = 0;
   tick.SourceHash = 0;

   long base = g_view + OFFSET_SLAVE_TICK;
   RtlMoveMemory(base, tick, TICK_DATA_BYTES);
   g_slaveTickSeq++;
   WriteU64(OFFSET_SLAVE_TICK, g_slaveTickSeq);
}

void EmitAccepted(const int side, const ulong ticket, const int errorCode)
{
   ExecutionReport rep;
   ZeroMemory(rep);
   rep.Seq = NextReportSeq();
   rep.Status = STATUS_ACCEPTED;
   rep.Side = EncodeSide(side);
   rep.Ticket = ticket;
   rep.FillPrice = 0.0;
   rep.FillLots = 0.0;
   rep.TsUs = (ulong)GetMicrosecondCount();
   rep.ErrorCode = errorCode;
   WriteExecutionReport(rep);
}

void EmitRejected(const int side, const ulong ticket, const int errorCode)
{
   ExecutionReport rep;
   ZeroMemory(rep);
   rep.Seq = NextReportSeq();
   rep.Status = STATUS_REJECTED;
   rep.Side = EncodeSide(side);
   rep.Ticket = ticket;
   rep.FillPrice = 0.0;
   rep.FillLots = 0.0;
   rep.TsUs = (ulong)GetMicrosecondCount();
   rep.ErrorCode = errorCode;
   WriteExecutionReport(rep);
}

void EmitFlat(const int side, const ulong ticket, const double fillPrice, const double fillLots)
{
   ExecutionReport rep;
   ZeroMemory(rep);
   rep.Seq = NextReportSeq();
   rep.Status = STATUS_FLAT;
   rep.Side = EncodeSide(side);
   rep.Ticket = ticket;
   rep.FillPrice = fillPrice;
   rep.FillLots = fillLots;
   rep.TsUs = (ulong)GetMicrosecondCount();
   rep.ErrorCode = 0;
   WriteExecutionReport(rep);
}

void WriteExecutionReport(const ExecutionReport &repIn)
{
   if (g_view == 0)
   {
      return;
   }

   // Two-phase write: payload first with Seq=0, then commit Seq as last field.
   ExecutionReport payload = repIn;
   payload.Seq = 0;

   long base = g_view + OFFSET_REPORT;
   RtlMoveMemory(base, payload, EXEC_REPORT_BYTES);

   ulong committedSeq = repIn.Seq;
   RtlMoveMemory(base + 0, committedSeq, 8);
}

ulong NextReportSeq()
{
   g_reportSeq++;
   return g_reportSeq;
}

uint ReadU32(const int offset)
{
   if (g_view == 0)
   {
      return 0;
   }

   uint value = 0;
   RtlMoveMemory(value, g_view + offset, 4);
   return value;
}

ulong ReadU64(const int offset)
{
   if (g_view == 0)
   {
      return 0;
   }

   ulong value = 0;
   RtlMoveMemory(value, g_view + offset, 8);
   return value;
}

void WriteU32(const int offset, const uint valueIn)
{
   if (g_view == 0)
   {
      return;
   }

   uint value = valueIn;
   RtlMoveMemory(g_view + offset, value, 4);
}

void WriteU64(const int offset, const ulong valueIn)
{
   if (g_view == 0)
   {
      return;
   }

   ulong value = valueIn;
   RtlMoveMemory(g_view + offset, value, 8);
}

uint EncodeSide(const int side)
{
   if (side > 0)
   {
      return 1;
   }

   if (side < 0)
   {
      return 0xFFFFFFFF;
   }

   return 0;
}

int DecodeSide(const uint side)
{
   if (side == 1)
   {
      return 1;
   }

   if (side == 0xFFFFFFFF)
   {
      return -1;
   }

   return 0;
}

int DigitsForSymbol(const string symbol)
{
   long digits = SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   if (digits < 0)
   {
      digits = _Digits;
   }

   return (int)digits;
}

ENUM_ORDER_TYPE_FILLING ResolveFillingMode(const string symbol)
{
   long mode = 0;
   if (!SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE, mode))
   {
      return ORDER_FILLING_IOC;
   }

   if ((mode & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC)
   {
      return ORDER_FILLING_IOC;
   }

   if ((mode & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK)
   {
      return ORDER_FILLING_FOK;
   }

   return ORDER_FILLING_RETURN;
}

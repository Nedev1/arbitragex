namespace ArbitrageX;

public static class NexusMmfLayout
{
    public const string MapName = @"Local\Nexus_MMF_v1";
    public const long MapSize = 256;

    public const long MasterTickOffset = 8;
    public const long TradeSignalOffset = 64;
    public const long ExecutionReportOffset = 128;
    public const long SlaveTickOffset = 192;

    public const long TickSeqOffset = 0;
    public const long TickBidOffset = 8;
    public const long TickAskOffset = 16;
    public const long TickTimestampOffset = 24;
    public const long TickSymbolHashOffset = 32;
    public const long TickSourceHashOffset = 36;
    public const long TickStructSize = 56;

    public const long SignalSeqOffset = 0;
    public const long SignalCommandOffset = 8;
    public const long SignalSideOffset = 12;
    public const long SignalLotsOffset = 16;
    // Compact signal layout keeps MMF at 256 bytes without overlapping report/tick slots.
    // Price/deviation are used for strict execution guardrails on MT5.
    public const long SignalPriceOffset = 24;
    public const long SignalMaxDeviationOffset = 32;
    public const long SignalReferenceOffset = 40;
    public const long SignalSlaveBidOffset = 48;
    public const long SignalSlaveAskOffset = 56;
    public const long SignalStructSize = 64;

    public const int SignalCommandNone = 0;
    public const int SignalCommandOpen = 1;
    public const int SignalCommandKill = 2;

    public const long ReportSeqOffset = 0;
    public const long ReportStatusOffset = 8;
    public const long ReportSideOffset = 12;
    public const long ReportTicketOffset = 16;
    public const long ReportFillPriceOffset = 24;
    public const long ReportFillLotsOffset = 32;
    public const long ReportTimestampOffset = 40;
    public const long ReportErrorCodeOffset = 56;
    public const long ReportStructSize = 64;

    public const int ReportStatusNone = 0;
    public const int ReportStatusAccepted = 1;
    public const int ReportStatusFilled = 2;
    public const int ReportStatusRejected = 3;
    public const int ReportStatusFlat = 4;
}

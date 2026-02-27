namespace ArbitrageX;

public sealed class EngineConfig
{
    public double LotSize { get; set; }
    public double MaxSpread { get; set; }
    public double TriggerThreshold { get; set; }
    public double HardStopLoss { get; set; }
    public bool UseRithmic { get; set; }
    public string? RithmicUsername { get; set; }
    public string? RithmicPassword { get; set; }
    public string? RithmicSystem { get; set; }
    public string? RithmicGateway { get; set; }
    public string? SourceTerminal { get; set; }
    public string? TargetTerminal { get; set; }
    public string? MasterSymbol { get; set; }
    public string? SlaveSymbol { get; set; }
}

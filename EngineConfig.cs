namespace ArbitrageX;

public sealed class EngineConfig
{
    public double LotSize { get; set; }
    public double MaxSpread { get; set; }
    public double TriggerThreshold { get; set; } = 1.5;
    public double HardStopLoss { get; set; }
    public double CalibrationAlpha { get; set; } = 0.001;
    public double MaxDeviationPoints { get; set; } = 2.0;
    public int MaxHoldTimeMs { get; set; } = 3000;
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

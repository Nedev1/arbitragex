using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Windows.Media;

namespace ArbitrageX;

public sealed class MainViewModel : INotifyPropertyChanged
{
    public ObservableCollection<string> Terminals { get; } = new();
    public ObservableCollection<string> DebugLog { get; } = new();

    private string _selectedSource = string.Empty;
    private string _selectedTarget = string.Empty;
    private string _masterSymbol = "NAS100";
    private string _slaveSymbol = "USTEC";

    private string _lotSize = "0.1";
    private string _maxSpread = "2.0";
    private string _triggerThreshold = "0.5";
    private string _hardStopLoss = "5.0";
    private bool _useRithmic;
    private string _rithmicUsername = string.Empty;
    private string _rithmicPassword = string.Empty;
    private string _rithmicSystem = string.Empty;
    private string _rithmicGateway = string.Empty;

    private string _ticksPerSecond = "0";
    private string _gapMs = "0";
    private string _trafficText = "Idle";
    private Brush _trafficBrush = Brushes.IndianRed;
    private string _lastFill = "-";
    private string _statusText = "Idle";

    private string _analysisStatus = "Idle";
    private string _analysisResult = "";
    private string _analysisDurationMinutes = "1";
    private string _analysisProgress = "";
    private bool _lowCpuMode = true;

    public string SelectedSource { get => _selectedSource; set { _selectedSource = value; OnPropertyChanged(); } }
    public string SelectedTarget { get => _selectedTarget; set { _selectedTarget = value; OnPropertyChanged(); } }
    public string MasterSymbol { get => _masterSymbol; set { _masterSymbol = value; OnPropertyChanged(); } }
    public string SlaveSymbol { get => _slaveSymbol; set { _slaveSymbol = value; OnPropertyChanged(); } }

    public string LotSize { get => _lotSize; set { _lotSize = value; OnPropertyChanged(); } }
    public string MaxSpread { get => _maxSpread; set { _maxSpread = value; OnPropertyChanged(); } }
    public string TriggerThreshold { get => _triggerThreshold; set { _triggerThreshold = value; OnPropertyChanged(); } }
    public string HardStopLoss { get => _hardStopLoss; set { _hardStopLoss = value; OnPropertyChanged(); } }
    public bool UseRithmic
    {
        get => _useRithmic;
        set
        {
            if (_useRithmic != value)
            {
                _useRithmic = value;
                OnPropertyChanged();

                if (_useRithmic)
                {
                    SelectedSource = "RITHMIC-FAST";
                }
                else if (SelectedSource == "RITHMIC-FAST")
                {
                    SelectedSource = string.Empty;
                    AutoSelectTerminals();
                }
            }
        }
    }
    public string RithmicUsername { get => _rithmicUsername; set { _rithmicUsername = value; OnPropertyChanged(); } }
    public string RithmicPassword { get => _rithmicPassword; set { _rithmicPassword = value; OnPropertyChanged(); } }
    public string RithmicSystem { get => _rithmicSystem; set { _rithmicSystem = value; OnPropertyChanged(); } }
    public string RithmicGateway { get => _rithmicGateway; set { _rithmicGateway = value; OnPropertyChanged(); } }

    public string TicksPerSecond { get => _ticksPerSecond; set { _ticksPerSecond = value; OnPropertyChanged(); } }
    public string GapMs { get => _gapMs; set { _gapMs = value; OnPropertyChanged(); } }
    public string TrafficText { get => _trafficText; set { _trafficText = value; OnPropertyChanged(); } }
    public Brush TrafficBrush { get => _trafficBrush; set { _trafficBrush = value; OnPropertyChanged(); } }
    public string LastFill { get => _lastFill; set { _lastFill = value; OnPropertyChanged(); } }
    public string StatusText { get => _statusText; set { _statusText = value; OnPropertyChanged(); } }

    public string AnalysisStatus { get => _analysisStatus; set { _analysisStatus = value; OnPropertyChanged(); } }
    public string AnalysisResult { get => _analysisResult; set { _analysisResult = value; OnPropertyChanged(); } }
    public string AnalysisDurationMinutes { get => _analysisDurationMinutes; set { _analysisDurationMinutes = value; OnPropertyChanged(); } }
    public string AnalysisProgress { get => _analysisProgress; set { _analysisProgress = value; OnPropertyChanged(); } }
    public bool LowCpuMode { get => _lowCpuMode; set { _lowCpuMode = value; OnPropertyChanged(); } }

    public EngineConfig ToConfig()
    {
        return new EngineConfig
        {
            LotSize = ParseDouble(LotSize, 0.1),
            MaxSpread = ParseDouble(MaxSpread, 2.0),
            TriggerThreshold = ParseDouble(TriggerThreshold, 0.5),
            HardStopLoss = ParseDouble(HardStopLoss, 5.0),
            UseRithmic = UseRithmic,
            RithmicUsername = string.IsNullOrWhiteSpace(RithmicUsername) ? null : RithmicUsername.Trim(),
            RithmicPassword = string.IsNullOrWhiteSpace(RithmicPassword) ? null : RithmicPassword,
            RithmicSystem = string.IsNullOrWhiteSpace(RithmicSystem) ? null : RithmicSystem.Trim(),
            RithmicGateway = string.IsNullOrWhiteSpace(RithmicGateway) ? null : RithmicGateway.Trim(),
            SourceTerminal = SelectedSource?.Trim(),
            TargetTerminal = SelectedTarget?.Trim(),
            MasterSymbol = MasterSymbol?.Trim(),
            SlaveSymbol = SlaveSymbol?.Trim()
        };
    }

    public void SyncTerminals(string[] terminals)
    {
        var changed = false;
        foreach (var t in terminals)
        {
            if (!Terminals.Contains(t))
            {
                Terminals.Add(t);
                changed = true;
            }
        }

        if (changed)
        {
            OnPropertyChanged(nameof(Terminals));
        }

        AutoSelectTerminals();
    }

    public void AddTerminal(string terminalId)
    {
        if (string.IsNullOrWhiteSpace(terminalId)) return;
        if (!Terminals.Contains(terminalId))
        {
            Terminals.Add(terminalId);
            OnPropertyChanged(nameof(Terminals));
        }

        AutoSelectTerminals();
    }

    public void AddLog(string message)
    {
        if (string.IsNullOrWhiteSpace(message)) return;
        var line = $"{DateTime.Now:HH:mm:ss.fff} {message}";
        DebugLog.Add(line);
        if (DebugLog.Count > 500)
        {
            DebugLog.RemoveAt(0);
        }
        OnPropertyChanged(nameof(DebugLog));
    }

    public TimeSpan GetAnalysisDuration()
    {
        if (double.TryParse(AnalysisDurationMinutes, out var v) && v > 0)
        {
            return TimeSpan.FromMinutes(v);
        }
        return TimeSpan.FromMinutes(1);
    }

    public double GetMaxSpread()
    {
        return ParseDouble(MaxSpread, 2.0);
    }

    public double GetTriggerThreshold()
    {
        return ParseDouble(TriggerThreshold, 0.5);
    }

    private static double ParseDouble(string value, double fallback)
    {
        if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var inv))
        {
            return inv;
        }

        if (double.TryParse(value, out var local))
        {
            return local;
        }

        return fallback;
    }

    private void AutoSelectTerminals()
    {
        if (Terminals.Count == 0) return;

        if (!UseRithmic && string.IsNullOrWhiteSpace(SelectedSource))
        {
            SelectedSource = Terminals[0];
        }

        if (string.IsNullOrWhiteSpace(SelectedTarget))
        {
            if (Terminals.Count > 1)
            {
                SelectedTarget = string.Equals(Terminals[0], SelectedSource, StringComparison.OrdinalIgnoreCase)
                    ? Terminals[1]
                    : Terminals[0];
            }
            else
            {
                SelectedTarget = Terminals[0];
            }
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Windows;
using System.Windows.Media;
using System.Windows.Threading;

namespace ArbitrageX;

public partial class MainWindow : Window
{
    private readonly MmfTickPublisher _mmfTickPublisher;
    private readonly RithmicFeed _rithmicFeed;
    private readonly HftEngine _engine;
    private readonly MainViewModel _vm;
    private readonly DispatcherTimer _uiTimer;
    private readonly ConcurrentQueue<string> _logQueue = new();
    private readonly ConcurrentQueue<HftExecutionReport> _executionQueue = new();
    private readonly string _debugLogFilePath = System.IO.Path.Combine(AppContext.BaseDirectory, "debug.log");

    public MainWindow()
    {
        InitializeComponent();

        try
        {
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;
        }
        catch
        {
            // Priority escalation can fail without elevated permissions.
        }

        _vm = new MainViewModel();
        DataContext = _vm;
        EnvLoader.Load(System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ".env"));
        _vm.RithmicUsername = Environment.GetEnvironmentVariable("RITHMIC_USERNAME") ?? "";
        _vm.RithmicPassword = Environment.GetEnvironmentVariable("RITHMIC_PASSWORD") ?? "";
        _vm.RithmicSystem = Environment.GetEnvironmentVariable("RITHMIC_SYSTEM") ?? "Rithmic Paper Trading";
        _vm.RithmicGateway = Environment.GetEnvironmentVariable("RITHMIC_GATEWAY") ?? "Chicago Area";
        _vm.UseRithmic = !string.IsNullOrWhiteSpace(_vm.RithmicUsername);

        _vm.AddTerminal("RITHMIC-FAST");
        _vm.AddTerminal("MT5-MMF");
        _vm.SelectedSource = "RITHMIC-FAST";
        if (string.IsNullOrWhiteSpace(_vm.SelectedTarget))
        {
            _vm.SelectedTarget = "MT5-MMF";
        }

        _mmfTickPublisher = new MmfTickPublisher();
        _rithmicFeed = new RithmicFeed(_mmfTickPublisher);
        _engine = new HftEngine();
        _engine.Log += OnEngineLog;
        _engine.ExecutionReportReceived += OnExecutionReportReceived;
        _vm.StatusText = "Awaiting BridgeMMF.mq5 slave ticks at MMF offset 192.";

        _uiTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(200) };
        _uiTimer.Tick += (_, _) => RefreshUi();
        _uiTimer.Start();

        AsyncLogger.Start(_debugLogFilePath);
        AppendFileLog("=== ArbitrageX started (MMF mode) ===");

        Closed += OnWindowClosed;
    }

    private void RefreshUi()
    {
        _vm.TicksPerSecond = _engine.CurrentTps.ToString();
        _vm.GapMs = _engine.CurrentGap.ToString("F5");

        int logsToDisplay = 50;
        string? lastLog = null;

        while (_logQueue.TryDequeue(out var logMsg))
        {
            if (logsToDisplay > 0)
            {
                if (!_vm.LowCpuMode) _vm.AddLog(logMsg);
                logsToDisplay--;
            }
            lastLog = logMsg;
        }
        if (lastLog != null) _vm.StatusText = lastLog;

        while (_executionQueue.TryDequeue(out var report))
        {
            var sideText = report.Side > 0 ? "BUY" : report.Side < 0 ? "SELL" : "N/A";
            _vm.LastFill =
                $"Seq={report.Seq} Status={report.Status} {sideText} Lots={report.FillLots:F2} @ {report.FillPrice:F5} Ticket={report.Ticket} Err={report.ErrorCode}";
        }

        if (_engine.TradingEnabled)
        {
            _vm.TrafficText = "Trading";
            _vm.TrafficBrush = Brushes.LimeGreen;
        }
        else
        {
            _vm.TrafficText = "Stopped";
            _vm.TrafficBrush = Brushes.IndianRed;
        }
    }

    private void Start_Click(object sender, RoutedEventArgs e)
    {
        if (!TryPrepareConfig(out var config))
        {
            return;
        }

        _engine.UpdateConfig(config);
        _engine.Start();
        _logQueue.Enqueue("MMF engine started. Expecting MT5 BridgeMMF.mq5 at Local\\Nexus_MMF_v1.");
        if (config.UseRithmic)
        {
            _ = _rithmicFeed.StartAsync(
                config.RithmicUsername ?? string.Empty,
                config.RithmicPassword ?? string.Empty,
                config.RithmicSystem ?? string.Empty,
                config.RithmicGateway ?? string.Empty,
                config.MasterSymbol ?? string.Empty);
        }
        else
        {
            _rithmicFeed.Stop();
            _logQueue.Enqueue("Rithmic feed disabled in config. Engine is running without master feed.");
        }
    }

    private void Stop_Click(object sender, RoutedEventArgs e)
    {
        _engine.Stop();
        _rithmicFeed.Stop();
        _vm.TrafficText = "Stopped";
        _vm.TrafficBrush = Brushes.IndianRed;
    }

    private void Kill_Click(object sender, RoutedEventArgs e)
    {
        _engine.KillSwitch();
        _vm.TrafficText = "Killed";
        _vm.TrafficBrush = Brushes.Red;
    }

    private void OnEngineLog(string message)
    {
        AppendFileLog("[ENGINE] " + message);
        _logQueue.Enqueue(message);
    }

    private void OnExecutionReportReceived(HftExecutionReport report)
    {
        _executionQueue.Enqueue(report);
    }

    private bool TryPrepareConfig(out EngineConfig config)
    {
        config = _vm.ToConfig();

        config.UseRithmic = _vm.UseRithmic;
        config.SourceTerminal = "RITHMIC-FAST";
        config.TargetTerminal = string.IsNullOrWhiteSpace(_vm.SelectedTarget) ? "MT5-MMF" : _vm.SelectedTarget.Trim();

        if (string.IsNullOrWhiteSpace(config.MasterSymbol) || string.IsNullOrWhiteSpace(config.SlaveSymbol))
        {
            _vm.StatusText = "Set Master and Slave symbols.";
            _vm.AddLog("[CONFIG] Missing Master/Slave symbol.");
            return false;
        }

        if (!config.UseRithmic)
        {
            _vm.StatusText = "Enable Rithmic feed for MMF mode.";
            _vm.AddLog("[CONFIG] Rithmic feed is required for MMF arbitrage mode.");
            return false;
        }

        _vm.AddLog($"[CONFIG] Source={config.SourceTerminal} Target={config.TargetTerminal} Master={config.MasterSymbol} Slave={config.SlaveSymbol}");
        AppendFileLog($"[CONFIG] Source={config.SourceTerminal} Target={config.TargetTerminal} Master={config.MasterSymbol} Slave={config.SlaveSymbol}");
        return true;
    }

    private void AppendFileLog(string message)
    {
        AsyncLogger.Enqueue(message);
    }

    private void Analyze_Click(object sender, RoutedEventArgs e)
    {
        _vm.AnalysisStatus = "Disabled in MMF mode";
        _vm.AnalysisProgress = "";
        _vm.AnalysisResult = "Analysis workflow from ArbitrageEngine is disabled in ArbitrageX 2.0 MMF mode.";
        AppendFileLog("[ANALYSIS] Disabled in MMF mode.");
    }

    private void OnWindowClosed(object? sender, EventArgs e)
    {
        try
        {
            _uiTimer.Stop();
        }
        catch
        {
            // ignored
        }

        try
        {
            _engine.Stop();
        }
        catch
        {
            // ignored
        }

        try
        {
            _rithmicFeed.Stop();
        }
        catch
        {
            // ignored
        }

        _engine.ExecutionReportReceived -= OnExecutionReportReceived;
        _engine.Log -= OnEngineLog;
        _engine.Dispose();
        _rithmicFeed.Dispose();
        _mmfTickPublisher.Dispose();
    }
}

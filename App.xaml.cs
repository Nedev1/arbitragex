using System;
using System.IO;
using System.Windows;
using System.Windows.Threading;

namespace ArbitrageX;

public partial class App : Application
{
    public App()
    {
        DispatcherUnhandledException += OnDispatcherUnhandledException;
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
    }

    private static void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        LogError("DispatcherUnhandledException", e.Exception);
        MessageBox.Show(e.Exception.ToString(), "ArbitrageX Crash", MessageBoxButton.OK, MessageBoxImage.Error);
        e.Handled = true;
        Current.Shutdown();
    }

    private static void OnUnhandledException(object? sender, UnhandledExceptionEventArgs e)
    {
        if (e.ExceptionObject is Exception ex)
        {
            LogError("UnhandledException", ex);
        }
        else
        {
            LogError("UnhandledException", new Exception("Unknown unhandled exception"));
        }
    }

    private static void LogError(string tag, Exception ex)
    {
        try
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "app_crash.log");
            File.AppendAllText(path, $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {tag}: {ex}\r\n");
        }
        catch
        {
            // ignored
        }
    }
}

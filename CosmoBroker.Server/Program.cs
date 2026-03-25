using System;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using CosmoBroker;

namespace CosmoBroker.Server;

class Program
{
    static async Task Main(string[] args)
    {
        // Pre-warm the thread pool to avoid the ~500 ms/thread spin-up latency under
        // connection bursts. Default minimum is Environment.ProcessorCount, which is
        // too low for a broker that gets hundreds of simultaneous connects.
        int cpus = Environment.ProcessorCount;
        ThreadPool.SetMinThreads(workerThreads: cpus * 8, completionPortThreads: cpus * 4);

        // SustainedLowLatency asks the GC to keep Gen2 collections rare,
        // trading peak memory for lower tail latency on the hot path.
        GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

        Console.WriteLine("Starting CosmoBroker Standalone...");

        var port = 4222;
        var monitorPort = 8222;

        var envPort = Environment.GetEnvironmentVariable("COSMOBROKER_PORT");
        if (!string.IsNullOrWhiteSpace(envPort) && int.TryParse(envPort, out var envParsed))
            port = envParsed;
        else if (args.Length > 0 && int.TryParse(args[0], out var argPort))
            port = argPort;

        var envMonitor = Environment.GetEnvironmentVariable("COSMOBROKER_MONITOR_PORT");
        if (!string.IsNullOrWhiteSpace(envMonitor) && int.TryParse(envMonitor, out var envMonParsed))
            monitorPort = envMonParsed;
        else if (args.Length > 1 && int.TryParse(args[1], out var argMonPort))
            monitorPort = argMonPort;

        var server = new BrokerServer(port: port, monitorPort: monitorPort);
        var cts = new CancellationTokenSource();

        // Handle both Ctrl+C (SIGINT) and SIGTERM (docker stop / systemd)
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => cts.Cancel();

        try
        {
            await server.StartAsync(cts.Token);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[CosmoBroker] Failed to start: {ex.Message}");
            Environment.Exit(1);
        }

        Console.WriteLine($"[CosmoBroker] Server is running on port {port}. Press Ctrl+C to stop.");

        try
        {
            await Task.Delay(-1, cts.Token);
        }
        catch (OperationCanceledException) { }

        Console.WriteLine("[CosmoBroker] Shutting down gracefully...");

        // Lame-duck: notify clients to reconnect before closing the listen socket
        server.EnterLameDuckMode();
        await Task.Delay(500); // brief window for clients to reconnect

        using var shutdownTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await server.DisposeAsync().AsTask().WaitAsync(shutdownTimeout.Token);
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("[CosmoBroker] Shutdown timed out after 10s, forcing exit.");
        }

        Console.WriteLine("[CosmoBroker] Stopped.");
    }
}

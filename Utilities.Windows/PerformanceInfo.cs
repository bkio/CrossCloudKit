// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Utilities.Windows;

/// <summary>
/// Represents memory performance information for the system.
/// </summary>
public sealed record MemoryInfo
{
    /// <summary>
    /// Gets the available physical memory in bytes.
    /// </summary>
    public long AvailablePhysicalMemoryBytes { get; init; }

    /// <summary>
    /// Gets the total physical memory in bytes.
    /// </summary>
    public long TotalPhysicalMemoryBytes { get; init; }

    /// <summary>
    /// Gets the percentage of free memory.
    /// </summary>
    public double FreeMemoryPercentage { get; init; }

    /// <summary>
    /// Gets the percentage of occupied memory.
    /// </summary>
    public double OccupiedMemoryPercentage { get; init; }

    /// <summary>
    /// Gets the available physical memory formatted as a human-readable string.
    /// </summary>
    public string AvailablePhysicalMemoryFormatted => FormatByteSize(AvailablePhysicalMemoryBytes);

    /// <summary>
    /// Gets the total physical memory formatted as a human-readable string.
    /// </summary>
    public string TotalPhysicalMemoryFormatted => FormatByteSize(TotalPhysicalMemoryBytes);

    /// <summary>
    /// Gets the used physical memory in bytes.
    /// </summary>
    public long UsedPhysicalMemoryBytes => TotalPhysicalMemoryBytes - AvailablePhysicalMemoryBytes;

    /// <summary>
    /// Gets the used physical memory formatted as a human-readable string.
    /// </summary>
    public string UsedPhysicalMemoryFormatted => FormatByteSize(UsedPhysicalMemoryBytes);

    public override string ToString()
    {
        return $"Memory: {UsedPhysicalMemoryFormatted}/{TotalPhysicalMemoryFormatted} " +
               $"({OccupiedMemoryPercentage:F1}% used, {FreeMemoryPercentage:F1}% free)";
    }

    /// <summary>
    /// Formats a byte count as a human-readable string with appropriate units.
    /// </summary>
    /// <param name="bytes">The number of bytes</param>
    /// <returns>A formatted string with appropriate units</returns>
    public static string FormatByteSize(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
        
        if (bytes == 0) 
            return $"0 {units[0]}";

        var absoluteBytes = Math.Abs(bytes);
        var unitIndex = (int)Math.Floor(Math.Log(absoluteBytes, 1024));
        
        // Ensure we don't exceed our units array
        unitIndex = Math.Min(unitIndex, units.Length - 1);
        
        var value = Math.Round(absoluteBytes / Math.Pow(1024, unitIndex), 2);
        return $"{Math.Sign(bytes) * value:F2} {units[unitIndex]}";
    }
}

/// <summary>
/// Provides Windows-specific performance monitoring capabilities.
/// This class uses Windows Performance Data Helper (PDH) APIs to retrieve system performance information.
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class PerformanceMonitor : IDisposable, IAsyncDisposable
{
    private readonly SemaphoreSlim _semaphore;
    private readonly Timer? _updateTimer;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private MemoryInfo? _lastMemoryInfo;
    private Exception? _lastException;
    private bool _disposed;

    /// <summary>
    /// Gets the current memory performance information.
    /// Returns cached data if available, otherwise retrieves fresh data.
    /// </summary>
    public MemoryInfo? CurrentMemoryInfo => _lastMemoryInfo;

    /// <summary>
    /// Gets the last exception that occurred during performance data retrieval, if any.
    /// </summary>
    public Exception? LastException => _lastException;

    /// <summary>
    /// Gets or sets the action to call when errors occur.
    /// </summary>
    public Action<string>? ErrorHandler { get; set; }

    /// <summary>
    /// Initializes a new instance of the PerformanceMonitor class.
    /// </summary>
    /// <param name="updateInterval">The interval at which to automatically update performance data (default: 1 second)</param>
    /// <param name="errorHandler">Optional error handler for logging errors</param>
    public PerformanceMonitor(TimeSpan? updateInterval = null, Action<string>? errorHandler = null)
    {
        _semaphore = new SemaphoreSlim(1, 1);
        _cancellationTokenSource = new CancellationTokenSource();
        ErrorHandler = errorHandler;

        // Get initial data
        _ = Task.Run(async () =>
        {
            try
            {
                _lastMemoryInfo = await GetMemoryInfoAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _lastException = ex;
                LogError($"Failed to get initial memory info: {ex.Message}");
            }
        });

        // Start periodic updates if requested
        if (updateInterval.HasValue)
        {
            _updateTimer = new Timer(UpdateTimerCallback, null, updateInterval.Value, updateInterval.Value);
        }
    }

    /// <summary>
    /// Retrieves current memory performance information asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>Current memory performance information</returns>
    public async Task<MemoryInfo> GetMemoryInfoAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await Task.Run(() => GetMemoryInfoInternal(), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Retrieves current memory performance information synchronously.
    /// For performance-critical scenarios, prefer the async version.
    /// </summary>
    /// <returns>Current memory performance information</returns>
    public MemoryInfo GetMemoryInfo()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _semaphore.Wait();
        try
        {
            return GetMemoryInfoInternal();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private MemoryInfo GetMemoryInfoInternal()
    {
        try
        {
            var perfInfo = new PerformanceInformation();
            var structSize = Marshal.SizeOf<PerformanceInformation>();
            
            if (!GetPerformanceInfo(out perfInfo, structSize))
            {
                var error = Marshal.GetLastWin32Error();
                throw new InvalidOperationException($"GetPerformanceInfo failed with error code: {error}");
            }

            var pageSize = perfInfo.PageSize.ToInt64();
            var totalPhysicalMemory = perfInfo.PhysicalTotal.ToInt64() * pageSize;
            var availablePhysicalMemory = perfInfo.PhysicalAvailable.ToInt64() * pageSize;

            var freePercentage = totalPhysicalMemory > 0 ? (double)availablePhysicalMemory / totalPhysicalMemory * 100 : 0;
            var occupiedPercentage = 100 - freePercentage;

            var memoryInfo = new MemoryInfo
            {
                TotalPhysicalMemoryBytes = totalPhysicalMemory,
                AvailablePhysicalMemoryBytes = availablePhysicalMemory,
                FreeMemoryPercentage = freePercentage,
                OccupiedMemoryPercentage = occupiedPercentage
            };

            _lastMemoryInfo = memoryInfo;
            _lastException = null;
            return memoryInfo;
        }
        catch (Exception ex)
        {
            _lastException = ex;
            LogError($"Error retrieving performance information: {ex.Message}");
            
            // Return last known good data if available, otherwise throw
            return _lastMemoryInfo ?? throw new InvalidOperationException("Unable to retrieve performance information and no cached data available", ex);
        }
    }

    private void UpdateTimerCallback(object? state)
    {
        if (_disposed || _cancellationTokenSource.Token.IsCancellationRequested)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await GetMemoryInfoAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancelling
            }
            catch (Exception ex)
            {
                LogError($"Error in background update: {ex.Message}");
            }
        });
    }

    private void LogError(string message)
    {
        try
        {
            ErrorHandler?.Invoke($"[PerformanceMonitor] {message}");
        }
        catch
        {
            // Ignore errors in error handler
        }
    }

    /// <summary>
    /// Releases all resources used by the PerformanceMonitor.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _cancellationTokenSource.Cancel();
            _updateTimer?.Dispose();
            _semaphore.Dispose();
            _cancellationTokenSource.Dispose();
            _disposed = true;
        }
    }

    /// <summary>
    /// Asynchronously releases all resources used by the PerformanceMonitor.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _cancellationTokenSource.Cancel();
            
            if (_updateTimer is not null)
            {
                await _updateTimer.DisposeAsync().ConfigureAwait(false);
            }
            
            _semaphore.Dispose();
            _cancellationTokenSource.Dispose();
            _disposed = true;
        }
    }

    #region Windows API Interop

    [DllImport("psapi.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetPerformanceInfo([Out] out PerformanceInformation performanceInformation, [In] int size);

    [StructLayout(LayoutKind.Sequential)]
    private struct PerformanceInformation
    {
        public int Size;
        public IntPtr CommitTotal;
        public IntPtr CommitLimit;
        public IntPtr CommitPeak;
        public IntPtr PhysicalTotal;
        public IntPtr PhysicalAvailable;
        public IntPtr SystemCache;
        public IntPtr KernelTotal;
        public IntPtr KernelPaged;
        public IntPtr KernelNonPaged;
        public IntPtr PageSize;
        public int HandlesCount;
        public int ProcessCount;
        public int ThreadCount;
    }

    #endregion
}

/// <summary>
/// Provides static utility methods for Windows performance monitoring.
/// For more advanced scenarios, use PerformanceMonitor class directly.
/// </summary>
[SupportedOSPlatform("windows")]
public static class PerformanceInfo
{
    private static readonly Lazy<PerformanceMonitor> _defaultMonitor = new(() => new PerformanceMonitor());

    /// <summary>
    /// Gets the default performance monitor instance.
    /// </summary>
    public static PerformanceMonitor Default => _defaultMonitor.Value;

    /// <summary>
    /// Gets current memory information using the default monitor.
    /// </summary>
    /// <param name="errorHandler">Optional error handler</param>
    /// <returns>Current memory information</returns>
    public static MemoryInfo GetMemoryInfo(Action<string>? errorHandler = null)
    {
        if (errorHandler is not null)
        {
            Default.ErrorHandler = errorHandler;
        }
        
        return Default.GetMemoryInfo();
    }

    /// <summary>
    /// Gets current memory information asynchronously using the default monitor.
    /// </summary>
    /// <param name="errorHandler">Optional error handler</param>
    /// <param name="cancellationToken">Cancellation token to observe</param>
    /// <returns>Current memory information</returns>
    public static async Task<MemoryInfo> GetMemoryInfoAsync(Action<string>? errorHandler = null, CancellationToken cancellationToken = default)
    {
        if (errorHandler is not null)
        {
            Default.ErrorHandler = errorHandler;
        }
        
        return await Default.GetMemoryInfoAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Formats a byte count as a human-readable string.
    /// This is a convenience method that delegates to MemoryInfo.FormatByteSize.
    /// </summary>
    /// <param name="bytes">The number of bytes</param>
    /// <returns>A formatted string with appropriate units</returns>
    public static string FormatByteSize(long bytes) => MemoryInfo.FormatByteSize(bytes);
}

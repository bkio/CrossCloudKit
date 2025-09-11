// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Runtime.InteropServices;

namespace CrossCloudKit.Utilities.Common
{
    /// <summary>
    /// A wrapper around Mutex that automatically acquires on construction and
    /// releases+disposes on disposal. Works for both named and unnamed mutexes.
    /// Handles name normalization for Linux vs. Windows.
    /// </summary>
    public sealed class AutoMutex : IDisposable
    {
        private readonly Mutex _mutex;
        private bool _hasHandle;

        /// <summary>
        /// Creates an unnamed mutex and acquires it immediately.
        /// </summary>
        public AutoMutex(TimeSpan? timeout = null)
            : this(null, timeout)
        {
        }

        /// <summary>
        /// Creates a named or unnamed mutex and acquires it immediately.
        /// On Linux/macOS, "Global\" and "Local\" prefixes are stripped automatically.
        /// Example:
        /// using var mutex = new AutoMutex(); //Unnamed mutex
        /// using var mutex = new AutoMutex("my-mutex"); //Named mutex
        /// The inner mutex will be locked, unlocked and disposed automatically.
        /// </summary>
        /// <param name="name">Name of the mutex, or null for unnamed.</param>
        /// <param name="timeout">Optional timeout for acquisition.</param>
        public AutoMutex(string? name, TimeSpan? timeout = null)
        {
            var normalizedName = NormalizeName(name);

            _mutex = string.IsNullOrEmpty(normalizedName)
                ? new Mutex(false)
                : new Mutex(false, normalizedName);

            _hasHandle = timeout.HasValue
                ? _mutex.WaitOne(timeout.Value, false)
                : _mutex.WaitOne();

            if (_hasHandle) return;
            _mutex.Dispose();
            throw new TimeoutException(
                name is null
                    ? "Could not acquire unnamed mutex within the timeout."
                    : $"Could not acquire the mutex '{name}' within the timeout."
            );
        }

        /// <summary>
        /// Normalize mutex names depending on OS.
        /// </summary>
        private static string? NormalizeName(string? name)
        {
            if (string.IsNullOrEmpty(name))
                return null;

            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? name
                : name.StartsWith("Global\\", StringComparison.OrdinalIgnoreCase)
                    ? name["Global\\".Length..]
                    : name.StartsWith("Local\\", StringComparison.OrdinalIgnoreCase)
                        ? name["Local\\".Length..]
                        : name;
        }

        public void Dispose()
        {
            if (_hasHandle)
            {
                try
                {
                    _mutex.ReleaseMutex();
                }
                catch (ApplicationException)
                {
                    // In case ReleaseMutex is called without ownership
                }
                _hasHandle = false;
            }

            _mutex.Dispose();
        }
    }
}

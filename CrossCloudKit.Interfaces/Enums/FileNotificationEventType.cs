// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Defines the types of pub/sub notification events for file operations.
/// </summary>
public enum FileNotificationEventType
{
    /// <summary>
    /// Event triggered when a file is uploaded/created.
    /// </summary>
    Uploaded,

    /// <summary>
    /// Event triggered when a file is deleted.
    /// </summary>
    Deleted
}


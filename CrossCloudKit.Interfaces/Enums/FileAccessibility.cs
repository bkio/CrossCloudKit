// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Enums;

/// <summary>
/// Defines the accessibility level for uploaded or copied files in cloud storage.
/// </summary>
public enum FileAccessibility
{
    /// <summary>
    /// File can only be accessed by authenticated users with proper permissions.
    /// </summary>
    AuthenticatedRead,

    /// <summary>
    /// File can be accessed by any user within the same project/organization.
    /// </summary>
    ProjectWideProtectedRead,

    /// <summary>
    /// File can be accessed publicly by anyone with the URL.
    /// </summary>
    PublicRead
}


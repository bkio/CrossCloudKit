// Copyright (c) 2022- Burak Kara, AGPL-3.0 license
// See LICENSE file in the project root for full license information.

using System.Reflection;

namespace CrossCloudKit.Utilities.Common;

public static class ExceptionExtensions
{
    public static T NotNull<T>(this T? value) where T : struct =>
        value ?? throw new NullReferenceException("Unexpected null value.");

    public static T NotNull<T>(this T? value) where T : class =>
        value ?? throw new NullReferenceException("Unexpected null value.");
}

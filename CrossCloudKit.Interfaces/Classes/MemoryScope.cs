// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Extend this interface to define a memory scope or use <see cref="MemoryScopeLambda"/>
/// </summary>
/// <remarks>
/// A memory scope is a namespace key that groups related key-value pairs in <see cref="IMemoryService"/>.
/// All memory operations (get, set, delete, lock) are scoped to a memory scope. Think of it as a
/// Redis hash key or a dictionary name.
/// </remarks>
public interface IMemoryScope
{
    public string Compile();
}

/// <summary>
/// A flexible implementation of IMemoryScope that allows defining memory scopes
/// using lambda functions or precompiled strings. This enables dynamic scope generation
/// at runtime or simple static scope definitions.
/// </summary>
/// <example>
/// <code>
/// // Static scope
/// var scope = new MemoryScopeLambda("user:123");
///
/// // Dynamic scope built at runtime
/// var userId = GetCurrentUserId();
/// var dynamicScope = new MemoryScopeLambda(() => $"user:{userId}");
///
/// // Use with memory operations
/// await memoryService.SetKeyValuesAsync(scope, new[] {
///     new KeyValuePair&lt;string, Primitive&gt;("name", new Primitive("John"))
/// });
/// </code>
/// </example>
public class MemoryScopeLambda : IMemoryScope
{
    private readonly Func<string> _compile;
    /// <summary>
    /// Creates a memory scope from a lambda that is evaluated each time <see cref="Compile"/> is called.
    /// </summary>
    /// <param name="compile">A function returning the scope key string.</param>
    public MemoryScopeLambda(Func<string> compile)
    {
        _compile = compile;
    }
    /// <summary>
    /// Creates a memory scope from a precompiled string value.
    /// </summary>
    /// <param name="precompiled">The fixed scope key string.</param>
    public MemoryScopeLambda(string precompiled)
    {
        _compile = () => precompiled;
    }
    /// <inheritdoc />
    public string Compile() => _compile();
}

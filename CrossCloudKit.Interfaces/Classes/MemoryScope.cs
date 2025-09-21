// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// Extend this interface to define a memory scope or use <see cref="MemoryScopeLambda"/>
/// </summary>
public interface IMemoryScope
{
    public string Compile();
}

/// <summary>
/// A flexible implementation of IMemoryScope that allows defining memory scopes
/// using lambda functions or precompiled strings. This enables dynamic scope generation
/// at runtime or simple static scope definitions.
/// </summary>
public class MemoryScopeLambda : IMemoryScope
{
    private readonly Func<string> _compile;
    public MemoryScopeLambda(Func<string> compile)
    {
        _compile = compile;
    }
    public MemoryScopeLambda(string precompiled)
    {
        _compile = () => precompiled;
    }
    public string Compile() => _compile();
}

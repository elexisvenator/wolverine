using JasperFx.CodeGeneration.Model;
using JasperFx.Core.Reflection;

namespace Wolverine.Marten.Codegen;

/// <summary>
/// The natural key as the generated code passes it to Marten. The fetch APIs take <c>TKey</c>, never
/// <c>TKey?</c>, so a member that can be null is checked first, the way a null string stream id is
/// treated: a write refuses it, and a read finds nothing.
/// </summary>
internal class NaturalKeyArgument
{
    private readonly Variable _member;
    private readonly bool _isNullableStruct;

    public NaturalKeyArgument(Variable member)
    {
        _member = member;

        var underlying = Nullable.GetUnderlyingType(member.VariableType);
        _isNullableStruct = underlying != null;
        KeyType = underlying ?? member.VariableType;
        CanBeNull = _isNullableStruct || !member.VariableType.IsValueType;
    }

    /// <summary>
    /// <c>TKey</c>, the type argument for <c>FetchForWriting&lt;T, TKey&gt;</c> and the other fetch APIs.
    /// </summary>
    public Type KeyType { get; }

    public bool CanBeNull { get; }

    public string Usage => _isNullableStruct ? $"{_member.Usage}.Value" : _member.Usage;

    public string FSharpUsage => _isNullableStruct ? $"{_member.FSharpUsage}.Value" : _member.FSharpUsage;

    public string IsNullCheck => $"{_member.Usage} == null";

    public string FSharpIsNullCheck => $"isNull (box {_member.FSharpUsage})";

    public string NullKeyMessage(Type aggregateType) =>
        $"The {KeyType.FullNameInCode()} natural key on this message is null, so there is no {aggregateType.FullNameInCode()} to load";

    /// <summary>
    /// A write has no aggregate to load and no key to claim, as Marten refuses a null string stream id.
    /// </summary>
    public string ThrowIfNull(Type aggregateType) =>
        $"if ({IsNullCheck}) throw new System.InvalidOperationException(\"{NullKeyMessage(aggregateType)}\");";

    public string FSharpThrowIfNull(Type aggregateType) =>
        $"if {FSharpIsNullCheck} then raise (System.InvalidOperationException(\"{NullKeyMessage(aggregateType)}\"))";
}

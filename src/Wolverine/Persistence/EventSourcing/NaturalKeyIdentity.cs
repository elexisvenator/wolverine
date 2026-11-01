using System.Diagnostics.CodeAnalysis;
using JasperFx;
using JasperFx.CodeGeneration.Model;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Wolverine.Configuration;

namespace Wolverine.Persistence.EventSourcing;

/// <summary>
///     Finds the message member that carries one of an aggregate's natural keys, for <c>[WriteModel]</c> and
///     <c>[ReadModel]</c> alike: its owned key, or any key it maps to another projection's lookup. A struct key
///     also matches a <c>TKey?</c> member.
/// </summary>
internal static class NaturalKeyIdentity
{
    /// <param name="findByType">
    ///     The attribute's own identity matching for one type: its explicit argument name, <c>[Identity]</c>,
    ///     the naming conventions and a single member of that type.
    /// </param>
    /// <remarks>
    ///     GH-4439: the chain goes over the seam because natural keys are registered per store. Without it the
    ///     store side can only ask the default store, and an aggregate registered on an ancillary store never
    ///     takes this branch.
    /// </remarks>
    public static Variable? Find(IEventSourcingFrameProvider provider, Type aggregateType, IChain chain,
        IServiceContainer container, Func<Type, Variable?> findByType)
    {
        var keyTypes = new List<Type>();
        if (provider.TryDetermineNaturalKeyType(aggregateType, chain, container) is { } owned)
        {
            keyTypes.Add(owned);
        }

        keyTypes.AddRange(provider.DetermineMappedNaturalKeyTypes(aggregateType, chain, container));

        var matches = keyTypes
            .Select(keyType => findMember(keyType, findByType))
            .OfType<Variable>()
            .DistinctBy(x => x.Usage)
            .ToArray();

        // Each key of an aggregate has its own type, so a message carrying two of them names two different
        // lookups, and silently taking the first would load by the wrong one.
        if (matches.Length > 1)
        {
            throw new InvalidOperationException(
                $"The message for {aggregateType.FullNameInCode()} on {chain.HandlerCalls().First()} carries more than one natural key of the aggregate ({matches.Select(x => x.Usage).Join(", ")}), so which one identifies it is ambiguous. Name the member to use in the attribute, or mark it with [Identity].");
        }

        return matches.SingleOrDefault();
    }

    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Closes Nullable<T> over a natural key struct at codegen time only, which an AOT-published app pre-generates. See docs/guide/aot.md.")]
    private static Variable? findMember(Type keyType, Func<Type, Variable?> findByType)
    {
        if (findByType(keyType) is { } exact) return exact;

        // The key is fetched as TKey?, which the event store treats as the same key
        return keyType.IsValueType && Nullable.GetUnderlyingType(keyType) == null
            ? findByType(typeof(Nullable<>).MakeGenericType(keyType))
            : null;
    }
}

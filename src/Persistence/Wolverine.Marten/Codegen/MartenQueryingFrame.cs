using JasperFx;
using JasperFx.CodeGeneration;
using JasperFx.CodeGeneration.Frames;
using JasperFx.CodeGeneration.Model;
using Marten;
using Marten.Services.BatchQuerying;
using Wolverine.Persistence;

namespace Wolverine.Marten.Codegen;

internal interface IBatchableFrame
{
    void WriteCodeToEnlistInBatchQuery(GeneratedMethod method, ISourceWriter writer);
    void EnlistInBatchQuery(Variable batchQuery);

    /// <summary>
    ///     A non-exclusive fetch of an aggregate Marten fetches with its Async plan, which brackets its reads in
    ///     <c>begin transaction isolation level repeatable read read only</c> … <c>end</c>. Marten only accepts
    ///     those before everything else in a batch, where they share one bracket.
    /// </summary>
    bool BracketsItsReads => false;

    /// <summary>
    ///     An exclusive fetch, whose row lock has to survive the batch. It cannot share a batch with a frame that
    ///     <see cref="BracketsItsReads" />, because that bracket's <c>end</c> commits the transaction holding it.
    /// </summary>
    bool HoldsLock => false;
}

internal class MartenBatchingPolicy : IMethodPreCompilationPolicy
{
    public void Apply(IGeneratedMethod method)
    {
        var (i, frames) = sortThroughFrames(method);
        if (frames.Count <= 1) return;

        // Marten only accepts bracketing fetches before everything else in a batch, so they go first. The rest
        // keep the order they were declared in, which never changes what the handler sees: every result is
        // resolved through its own variable after the batch has executed.
        var bracketing = frames.Where(x => x.BracketsItsReads).ToArray();
        var locking = frames.Where(x => x.HoldsLock).ToArray();
        var others = frames.Except(bracketing).Except(locking).ToArray();

        if (bracketing.Length == 0 || locking.Length == 0)
        {
            var batchFrame = new MartenBatchFrame();
            method.Frames.Insert(i, batchFrame);

            foreach (var frame in bracketing.Concat(frames.Except(bracketing)))
            {
                batchFrame.Enlist(frame);
            }

            return;
        }

        // A bracket's `end` commits the session's transaction, which would release an exclusive fetch's row
        // lock, so the two cannot share a batch. The bracketing fetches go first, with everything else, while
        // the session has no transaction yet: an exclusive fetch starts one, and inside it a bracketing fetch
        // gets no bracket at all (#5611). The exclusive fetches follow in a second batch of their own, which
        // also holds their locks for as short a time as possible. Both batches are batch frames, even with one
        // item, so the second always runs after the first.
        var snapshotBatch = new MartenBatchFrame();
        var lockingBatch = new MartenBatchFrame("lockingBatchedQuery");
        method.Frames.Insert(i, lockingBatch);
        method.Frames.Insert(i, snapshotBatch);

        foreach (var frame in bracketing.Concat(others))
        {
            snapshotBatch.Enlist(frame);
        }

        foreach (var frame in locking)
        {
            lockingBatch.Enlist(frame);
        }
    }

    private static bool IsBatchable(IBatchableFrame frame)
    {
        // A query specification that implements only IQueryPlan<T> (no
        // IBatchQueryPlan<T>) must execute standalone.
        if (frame is FetchSpecificationFrame fsf && !fsf.CanBatch) return false;

        // GH-4505. A deduplication check on an OPTIONAL id has to stay standalone: batch enlistment is
        // unconditional code, and an unkeyed message is supposed to cost no round trip at all.
        if (frame is MartenDeduplicationClaimExistsFrame dedup && !dedup.CanBatch) return false;

        // GH-5160. The same for a [DeduplicatedWithResponse] claim lookup on an optional key.
        if (frame is MartenDeduplicatedResponseClaimFrame response && !response.CanBatch) return false;

        return true;
    }

    private static (int, IReadOnlyList<IBatchableFrame> frames) sortThroughFrames(IGeneratedMethod method)
    {
        var list = new List<IBatchableFrame>();
        
        var index = -1;
        for (int i = 0; i < method.Frames.Count; i++)
        {
            var frame = method.Frames[i];
            if (frame is LoadEntityFrameBlock block && block.Creator is IBatchableFrame b && IsBatchable(b))
            {
                list.Add(b);
                if (index == -1)
                {
                    index = i;
                }
            }
            else if (frame is IBatchableFrame batchable && IsBatchable(batchable))
            {
                list.Add(batchable);
                if (index == -1)
                {
                    index = i;
                }
            }
        }

        return (index, list);
    }
}

internal class MartenBatchFrame : AsyncFrame
{
    private Variable _session = null!;
    private Variable _cancellation = null!;

    private List<IBatchableFrame> _operations = new();

    public MartenBatchFrame()
    {
        BatchQuery = new Variable(typeof(IBatchedQuery), this);
    }

    /// <param name="batchQueryName">For a second batch in the same method, which needs its own variable name.</param>
    public MartenBatchFrame(string batchQueryName)
    {
        BatchQuery = new Variable(typeof(IBatchedQuery), batchQueryName, this);
    }

    public override void GenerateCode(GeneratedMethod method, ISourceWriter writer)
    {
        writer.WriteLine($"var {BatchQuery.Usage} = {_session.Usage}.{nameof(IQuerySession.CreateBatchQuery)}();");
        foreach (var op in _operations)
        {
            writer.BlankLine();
            op.WriteCodeToEnlistInBatchQuery(method, writer);
        }

        writer.BlankLine();

        writer.WriteLine($"await {BatchQuery.Usage}.{nameof(IBatchedQuery.Execute)}({_cancellation.Usage});");

        writer.BlankLine();

        // After the batch query executes, generate the code to resolve each
        // batched frame's result variable (e.g. var stream_entity = await stream_entity_BatchItem).
        // This must happen BEFORE any guard frames that reference these variables.
        foreach (var op in _operations)
        {
            if (op is LoadAggregateFrame loadFrame)
            {
                loadFrame.GenerateCodeForBatchResolution(method, writer);
            }
        }

        writer.BlankLine();

        Next?.GenerateCode(method, writer);
    }
    
    public Variable BatchQuery { get; }

    public void Enlist(IBatchableFrame frame)
    {
        if (_operations.Contains(frame)) return;
        
        frame.EnlistInBatchQuery(BatchQuery);
        _operations.Add(frame);
    }

    public override IEnumerable<Variable> FindVariables(IMethodVariables chain)
    {
        _session = chain.FindVariable(typeof(IDocumentSession));
        yield return _session;
        
        _cancellation = chain.FindVariable(typeof(CancellationToken));
        yield return _cancellation;

        // This ensures those variables are declared before we try to use them in the batch enlistment code
        foreach (var op in _operations)
        {
            if (op is Frame frame)
            {
                foreach (var variable in frame.FindVariables(chain))
                {
                    yield return variable;
                }
            }
        }
    }
}
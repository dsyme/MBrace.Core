﻿namespace MBrace.Runtime.InMemoryRuntime

#nowarn "444"

open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

open Nessos.FsPickler

/// Concurrent counter implementation
[<AutoSerializable(false); CloneableOnly>]
type private ConcurrentCounter (init : int) =
    [<VolatileField>]
    let mutable value = init
    /// Increments counter by 1
    member __.Increment() = Interlocked.Increment &value
    /// Gets the current counter value
    member __.Value = value

[<AutoSerializable(false); CloneableOnly>]
type private ResultAggregator<'T> (size : int) =
    let array = Array.zeroCreate<'T> size
    member __.Length = size
    member __.Values = array
    member __.Item with set i x = array.[i] <- x

/// Collection of workflows that provide parallelism
/// using the .NET thread pool
type ThreadPool private () =

    static let scheduleTask res ct sc ec cc wf =
        Trampoline.QueueWorkItem(fun () ->
            let ctx = { Resources = res ; CancellationToken = ct }
            let cont = { Success = sc ; Exception = ec ; Cancellation = cc }
            Cloud.StartWithContinuations(wf, cont, ctx))

    static let cloneProtected (memoryEmulation : MemoryEmulation) (value : 'T) =
        try EmulatedValue.clone memoryEmulation value |> Choice1Of2
        with e -> Choice2Of2 e

    static let emulateProtected (memoryEmulation : MemoryEmulation) (value : 'T) =
        try EmulatedValue.create memoryEmulation true value |> Choice1Of2
        with e -> Choice2Of2 e

    /// <summary>
    ///     A Cloud.Parallel implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Parallel (mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, memoryEmulation : MemoryEmulation, computations : seq<#Cloud<'T>>) : Cloud0<'T []> =
        Cloud0.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // early detect if return type is not serializable.
            | Choice1Of2 _ when not <| MemoryEmulation.isShared memoryEmulation && not <| FsPickler.IsSerializableType<'T>() ->     
                let msg = sprintf "Cloud.Parallel workflow uses non-serializable type '%s'." (Type.prettyPrint typeof<'T>)
                let e = new SerializationException(msg)
                cont.Exception ctx (ExceptionDispatchInfo.Capture e)

            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx [||]

            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] ->
                match cloneProtected memoryEmulation (comp, cont) with
                | Choice1Of2 (comp, cont) ->
                    let cont' = Continuation.map (fun t -> [| t |]) cont
                    Cloud.StartWithContinuations(comp, cont', ctx)

                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                match emulateProtected memoryEmulation (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Parallel<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 clonedComputations ->
                    let results = new ResultAggregator<'T>(computations.Length)
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let exceptionLatch = new ConcurrentCounter(0)
                    let completionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess (i : int) (cont : Continuation<'T[]>) ctx (t : 'T) =
                        match cloneProtected memoryEmulation t with
                        | Choice1Of2 t ->
                            results.[i] <- t
                            if completionLatch.Increment() = results.Length then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) results.Values

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." (Type.prettyPrint typeof<'T>) 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onException (cont : Continuation<'T[]>) ctx edi =
                        match cloneProtected memoryEmulation edi with
                        | Choice1Of2 edi ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel ()
                                cont.Exception (revertCtx ctx) edi

                        | Choice2Of2 e ->
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                let msg = sprintf "Cloud.Parallel<%s> workflow failed to serialize result." (Type.prettyPrint typeof<'T>) 
                                let se = new SerializationException(msg, e)
                                cont.Exception (revertCtx ctx) (ExceptionDispatchInfo.Capture se)

                    let onCancellation (cont : Continuation<'T[]>) ctx c =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) c

                    for i = 0 to computations.Length - 1 do
                        // clone different continuation for each child
                        let computations,cont = clonedComputations.Value
                        scheduleTask ctx.Resources innerCts.Token (onSuccess i cont) (onException cont) (onCancellation cont) computations.[i])

    /// <summary>
    ///     A Cloud.Choice implementation executed using the thread pool.
    /// </summary>
    /// <param name="mkNestedCts">Creates a child cancellation token source for child workflows.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="computations">Input computations.</param>
    static member Choice(mkNestedCts : ICloudCancellationToken -> ICloudCancellationTokenSource, memoryEmulation : MemoryEmulation, computations : seq<#Cloud<'T option>>) : Cloud0<'T option> =
        Cloud0.FromContinuations(fun ctx cont ->
            match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
            // handle computation sequence enumeration error
            | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
            // handle empty computation case directly.
            | Choice1Of2 [||] -> cont.Success ctx None
            // pass continuation directly to child, if singular
            | Choice1Of2 [| comp |] -> 
                match cloneProtected memoryEmulation (comp, cont) with
                | Choice1Of2 (comp, cont) -> Cloud.StartWithContinuations(comp, cont, ctx)
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

            | Choice1Of2 computations ->
                // distributed computation, ensure that closures are serializable
                match emulateProtected memoryEmulation (computations, cont) with
                | Choice2Of2 e ->
                    let msg = sprintf "Cloud.Choice<%s> workflow uses non-serializable closures." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    cont.Exception ctx (ExceptionDispatchInfo.Capture se)

                | Choice1Of2 clonedComputations ->
                    let N = computations.Length // avoid capturing original computations in continuation closures
                    let parentCt = ctx.CancellationToken
                    let innerCts = mkNestedCts parentCt
                    let completionLatch = new ConcurrentCounter(0)
                    let exceptionLatch = new ConcurrentCounter(0)

                    let inline revertCtx (ctx : ExecutionContext) = { ctx with CancellationToken = parentCt }

                    let onSuccess (cont : Continuation<'T option>) ctx (topt : 'T option) =
                        if Option.isSome topt then
                            if exceptionLatch.Increment() = 1 then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) topt
                        else
                            if completionLatch.Increment () = N then
                                innerCts.Cancel()
                                cont.Success (revertCtx ctx) None

                    let onException (cont : Continuation<'T option>) ctx edi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Exception (revertCtx ctx) edi

                    let onCancellation (cont : Continuation<'T option>) ctx cdi =
                        if exceptionLatch.Increment() = 1 then
                            innerCts.Cancel ()
                            cont.Cancellation (revertCtx ctx) cdi

                    for i = 0 to computations.Length - 1 do
                        // clone different continuation for each child
                        let computations,cont = clonedComputations.Value
                        scheduleTask ctx.Resources innerCts.Token (onSuccess cont) (onException cont) (onCancellation cont) computations.[i])

    /// <summary>
    ///     A Cloud.StartAsTask implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    /// <param name="cancellationToken">Cancellation token for task. Defaults to new cancellation token.</param>
    static member StartAsTask (workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry, ?cancellationToken : ICloudCancellationToken) =
        if memoryEmulation <> MemoryEmulation.Shared && not <| FsPickler.IsSerializableType<'T> () then
            let msg = sprintf "Cloud task returns non-serializable type '%s'." (Type.prettyPrint typeof<'T>)
            raise <| new SerializationException(msg)


        match cloneProtected memoryEmulation workflow with
        | Choice2Of2 e ->
            let msg = sprintf "Cloud task of type '%s' uses non-serializable closure." (Type.prettyPrint typeof<'T>)
            raise <| new SerializationException(msg, e)

        | Choice1Of2 workflow ->
            let clonedWorkflow = EmulatedValue.clone memoryEmulation workflow
            let tcs = new InMemoryTaskCompletionSource<'T>(?cancellationToken = cancellationToken)
            let setResult cont result =
                match cloneProtected memoryEmulation result with
                | Choice1Of2 result -> cont result |> ignore
                | Choice2Of2 e ->
                    let msg = sprintf "Could not serialize result for task of type '%s'." (Type.prettyPrint typeof<'T>)
                    let se = new SerializationException(msg, e)
                    ignore <| tcs.LocalTaskCompletionSource.TrySetException se

            let cont = 
                {
                    Success = fun _ t -> t |> setResult tcs.LocalTaskCompletionSource.TrySetResult
                    Exception = fun _ (edi:ExceptionDispatchInfo) -> edi |> setResult (fun edi -> tcs.LocalTaskCompletionSource.TrySetException (edi.Reify(false, false)))
                    Cancellation = fun _ _ -> tcs.LocalTaskCompletionSource.TrySetCanceled() |> ignore
                }

            Trampoline.QueueWorkItem(fun () -> Cloud.StartWithContinuations(clonedWorkflow, cont, resources, tcs.CancellationTokenSource.Token))
            tcs.Task


    /// <summary>
    ///     A Cloud.ToAsync implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    static member ToAsync(workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry) : Async<'T> = async {
        let! ct = Async.CancellationToken
        let imct = new InMemoryCancellationToken(ct)
        let task = ThreadPool.StartAsTask(workflow, memoryEmulation, resources, cancellationToken = imct)
        return! Async.AwaitTaskCorrect task.LocalTask
    }

    /// <summary>
    ///     A Cloud.ToAsync implementation executed using the thread pool.
    /// </summary>
    /// <param name="workflow">Workflow to be executed.</param>
    /// <param name="memoryEmulation">Memory semantics used for parallelism.</param>
    /// <param name="resources">Resource registry used for cloud workflow.</param>
    static member RunSynchronously(workflow : Cloud<'T>, memoryEmulation : MemoryEmulation, resources : ResourceRegistry, ?cancellationToken) : 'T =
        let task = ThreadPool.StartAsTask(workflow, memoryEmulation, resources, ?cancellationToken = cancellationToken)
        task.LocalTask.GetResult()
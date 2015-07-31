/// Assortment of workflow combinators that act on collections distributively.
module MBrace.Library.Cloud

open System.Collections.Generic

open MBrace.Core
open MBrace.Core.Internals

/// Collection combinators that operate sequentially on the inputs.
[<RequireQualifiedAccess>]
module Sequential =

    /// <summary>
    ///     Sequential map combinator.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Source sequence.</param>
    let map (mapper : 'T -> #Cloud<'S>) (source : seq<'T>) : Cloud<'S []> = cloud {
        let rec aux acc rest = cloud {
            match rest with
            | [] -> return acc |> List.rev |> List.toArray
            | t :: rest' ->
                let! s = mapper t
                return! aux (s :: acc) rest'
        }

        return! aux [] (Seq.toList source)
    }

    /// <summary>
    ///     Sequential filter combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let filter (predicate : 'T -> #Cloud<bool>) (source : seq<'T>) : Cloud<'T []> = cloud {
        let rec aux acc rest = cloud {
            match rest with
            | [] -> return acc |> List.rev |> List.toArray
            | t :: rest' ->
                let! r = predicate t
                return! aux (if r then t :: acc else acc) rest'
        }

        return! aux [] (List.ofSeq source)
    }

    /// <summary>
    ///     Sequential choose combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let choose (chooser : 'T -> #Cloud<'S option>) (source : seq<'T>) : Cloud<'S []> = cloud {
        let rec aux acc rest = cloud {
            match rest with
            | [] -> return acc |> List.rev |> List.toArray
            | t :: rest' ->
                let! r = chooser t
                return! aux (match r with Some s -> s :: acc | None -> acc) rest'
        }

        return! aux [] (List.ofSeq source)
    }

    /// <summary>
    ///     Sequential fold combinator.
    /// </summary>
    /// <param name="folder">Folding function.</param>
    /// <param name="state">Initial state.</param>
    /// <param name="source">Input data.</param>
    let fold (folder : 'State -> 'T -> #Cloud<'State>) (state : 'State) (source : seq<'T>) : Cloud<'State> = cloud {
        let rec aux state rest = cloud {
            match rest with
            | [] -> return state
            | t :: rest' ->
                let! state' = folder state t
                return! aux state' rest'
        }

        return! aux state (List.ofSeq source)
    }

    /// <summary>
    ///     Sequential eager collect combinator.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Source data.</param>
    let collect (collector : 'T -> #Cloud<#seq<'S>>) (source : seq<'T>) : Cloud<'S []> = cloud {
        let! results = map (fun t -> cloud { let! ss = collector t in return Seq.toArray ss }) source
        return Array.concat results
    }

    /// <summary>
    ///     Sequential tryFind combinator.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input sequence.</param>
    let tryFind (predicate : 'T -> #Cloud<bool>) (source : seq<'T>) : Cloud<'T option> = cloud {
        let rec aux rest = cloud {
            match rest with
            | [] -> return None
            | t :: rest' ->
                let! r = predicate t
                if r then return Some t
                else return! aux rest'
        }

        return! aux (List.ofSeq source)
    }

    /// <summary>
    ///     Sequential tryPick combinator.
    /// </summary>
    /// <param name="chooser">Choice function.</param>
    /// <param name="source">Input sequence.</param>
    let tryPick (chooser : 'T -> #Cloud<'S option>) (source : seq<'T>) : Cloud<'S option> = cloud {
        let rec aux rest = cloud {
            match rest with
            | [] -> return None
            | t :: rest' ->
                let! r = chooser t
                match r with
                | Some _ as s -> return s
                | None -> return! aux rest'
        }

        return! aux (List.ofSeq source)
    }

    /// <summary>
    ///     Sequential iter combinator.
    /// </summary>
    /// <param name="body">Iterator body.</param>
    /// <param name="source">Input sequence.</param>
    let iter (body : 'T -> #Cloud<unit>) (source : seq<'T>) : Cloud<unit> = cloud {
        let rec aux rest = cloud {
            match rest with
            | [] -> return ()
            | t :: rest' ->
                do! body t
                return! aux rest'
        }

        return! aux (List.ofSeq source)
    }


/// Set of parallel collection combinators that balance 
/// input data across the cluster according to worker processing capacities. 
/// Designed to minimize runtime overhead by bundling inputs in single jobs per worker,
/// they also utilize the multicore capacity of every worker machine.
/// It is assumed here that all inputs are homogeneous in terms of computation workloads.
[<RequireQualifiedAccess>]
module Balanced =

    let private lift f t = cloud0 { return f t }

    /// <summary>
    ///     General-purpose distributed reduce/combine combinator. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="reducer">Single-threaded reduce function. Reduces a materialized collection of inputs to an intermediate result.</param>
    /// <param name="combiner">Combiner function that aggregates intermediate results into one.</param>
    /// <param name="source">Input data.</param>
    let reduceCombine (reducer : 'T [] -> Cloud0<'State>) 
                        (combiner : 'State [] -> Cloud0<'State>) 
                        (source : seq<'T>) : Cloud<'State> =

        let reduceCombineLocal (inputs : 'T[]) = cloud0 {
            if inputs.Length < 2 then return! reducer inputs
            else
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                let! results =
                    chunks
                    |> Array.map reducer
                    |> Cloud0.Parallel

                return! combiner results
        }

        cloud {
            let inputs = Seq.toArray source
            if inputs.Length < 2 then return! reducer inputs
            else
                let! workers = Cloud.GetAvailableWorkers()
                let chunks = WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers inputs
                let! results =
                    chunks
                    |> Seq.filter (not << Array.isEmpty << snd)
                    |> Seq.map (fun (w,ts) -> reduceCombineLocal ts, w)
                    |> Cloud.Parallel

                return! combiner results
        }

    /// <summary>
    ///     Distributed map combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let mapCloud (mapper : 'T -> Cloud0<'S>) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (Cloud0.Sequential.map mapper) (lift Array.concat) source

    /// <summary>
    ///     Distributed map combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="mapper">Mapper function.</param>
    /// <param name="source">Input data.</param>
    let map (mapper : 'T -> 'S) (source : seq<'T>) : Cloud<'S []> = 
        reduceCombine (lift <| Array.map mapper) (lift Array.concat) source

    /// <summary>
    ///     Distributed filter combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filterCloud (predicate : 'T -> Cloud0<bool>) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (Cloud0.Sequential.filter predicate) (lift Array.concat) source

    /// <summary>
    ///     Distributed filter combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let filter (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T []> =
        reduceCombine (lift <| Array.filter predicate) (lift Array.concat) source

    /// <summary>
    ///     Distributed choose combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let chooseCloud (chooser : 'T -> Cloud0<'S option>) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (Cloud0.Sequential.choose chooser) (lift Array.concat) source

    /// <summary>
    ///     Distributed choose combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let choose (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S []> =
        reduceCombine (lift <| Array.choose chooser) (lift Array.concat) source

    /// <summary>
    ///     Distrbuted collect combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collectCloud (collector : 'T -> Cloud0<#seq<'S>>) (source : seq<'T>) =
        reduceCombine (Cloud0.Sequential.collect collector) (lift Array.concat) source

    /// <summary>
    ///     Distrbuted collect combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="collector">Collector function.</param>
    /// <param name="source">Input data.</param>
    let collect (collector : 'T -> #seq<'S>) (source : seq<'T>) =
        reduceCombine (lift <| Array.collect (Seq.toArray << collector)) (lift Array.concat) source

    /// <summary>
    ///     Distributed iter combinator. Computation is balanced across the
    ///     cluster according to multicore capacity.
    /// </summary>
    /// <param name="body">Iterator body.</param>
    /// <param name="source">Input sequence.</param>
    let iterCloud (body : 'T -> Cloud0<unit>) (source : seq<'T>) : Cloud<unit> =
        reduceCombine (Cloud0.Sequential.iter body) (fun _ -> cloud0.Zero()) source

    /// <summary>
    ///     Distributed fold combinator. Partitions inputs, folding distrbutively
    ///     and then combines the intermediate results. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="folder">Folding function.</param>
    /// <param name="reducer">Intermediate state reducing function.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input data.</param>
    let foldCloud (folder : 'State -> 'T -> Cloud0<'State>)
                    (reducer : 'State -> 'State -> Cloud0<'State>)
                    (init : 'State) (source : seq<'T>) : Cloud<'State> =

        reduceCombine (Cloud0.Sequential.fold folder init) (Cloud0.Sequential.fold reducer init) source

    /// <summary>
    ///     Distributed fold combinator. Partitions inputs, folding distrbutively
    ///     and then combines the intermediate results. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="folder">Folding function.</param>
    /// <param name="reducer">Intermediate state reducing function.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input data.</param>
    let fold (folder : 'State -> 'T -> 'State)
                (reducer : 'State -> 'State -> 'State)
                (init : 'State) (source : seq<'T>) : Cloud<'State> =

        let reduce inputs =
            if Array.isEmpty inputs then init
            else
                Array.reduce reducer inputs

        reduceCombine (lift <| Array.fold folder init) (lift reduce) source

    /// <summary>
    ///     Distributed fold by key combinator. Partitions inputs, folding distrbutively
    ///     and then combines the intermediate results. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="projection">Projection function to group inputs by.</param>
    /// <param name="folder">folding workflow.</param>
    /// <param name="reducer">State combining workflow.</param>
    /// <param name="init">State initializer workflow.</param>
    /// <param name="source">Input data.</param>
    let foldByCloud (projection : 'T -> 'Key) 
                    (folder : 'State -> 'T -> Cloud0<'State>)
                    (reducer : 'State -> 'State -> Cloud0<'State>)
                    (init : 'Key -> Cloud0<'State>) (source : seq<'T>) : Cloud<('Key * 'State) []> = 

        let reduce (inputs : 'T []) = cloud0 {
            let dict = new Dictionary<'Key, 'State ref> ()
            for t in inputs do
                let k = projection t
                let ok, s = dict.TryGetValue k
                let! stateRef = cloud0 {
                    if ok then return s 
                    else
                        let! init = init k
                        let ref = ref init
                        dict.Add(k, ref)
                        return ref
                }

                let! state' = folder !stateRef t
                stateRef := state'

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) |> Seq.toArray
        }

        let combine (results : ('Key * 'State) [][]) = cloud0 {
            let dict = new Dictionary<'Key, 'State ref> ()
            for k,state in Seq.concat results do
                let ok, stateRef = dict.TryGetValue k
                if ok then
                    let! state' = reducer !stateRef state
                    stateRef := state'
                else
                    dict.Add(k, ref state)

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) |> Seq.toArray
        }

        reduceCombine reduce combine source


    /// <summary>
    ///     Distributed fold by key combinator. Partitions inputs, folding distrbutively
    ///     and then combines the intermediate results. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="projection">Projection function to group inputs by.</param>
    /// <param name="folder">folding workflow.</param>
    /// <param name="reducer">State combining workflow.</param>
    /// <param name="init">State initializer workflow.</param>
    /// <param name="source">Input data.</param>
    let foldBy (projection : 'T -> 'Key) 
                (folder : 'State -> 'T -> 'State)
                (reducer : 'State -> 'State -> 'State)
                (init : 'Key -> 'State) (source : seq<'T>) : Cloud<('Key * 'State) []> = 

        let reduce (inputs : 'T []) = cloud0 {
            let dict = new Dictionary<'Key, 'State ref> ()
            do for t in inputs do
                let k = projection t
                let ok, s = dict.TryGetValue k
                let stateRef =
                    if ok then s 
                    else
                        let init = init k
                        let ref = ref init
                        dict.Add(k, ref)
                        ref

                let state' = folder !stateRef t
                stateRef := state'

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) |> Seq.toArray
        }

        let combine (results : ('Key * 'State) [][]) = cloud0 {
            let dict = new Dictionary<'Key, 'State ref> ()
            for result in results do
                for k,state in result do
                    let ok, stateRef = dict.TryGetValue k
                    if ok then
                        let state' = reducer !stateRef state
                        stateRef := state'
                    else
                        dict.Add(k, ref state)

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value.Value) |> Seq.toArray
        }

        reduceCombine reduce combine source


    /// <summary>
    ///     Distributed groupBy combinator. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="projection">Projection function to group values by.</param>
    /// <param name="source">Input data.</param>
    let groupBy (projection : 'T -> 'Key) (source : seq<'T>) =
        let reduce (inputs : 'T []) = cloud0 {
            let dict = new Dictionary<'Key, ResizeArray<'T>> ()
            do for t in inputs do
                let k = projection t
                let ok, result = dict.TryGetValue k
                let aggregator =
                    if ok then result
                    else
                        let agg = new ResizeArray<'T> ()
                        dict.Add(k, agg)
                        agg

                aggregator.Add t

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value.ToArray()) |> Seq.toArray
        }

        let combine (results : ('Key * 'T[]) [][]) = cloud0 {
            let dict = new Dictionary<'Key, ResizeArray<'T[]>> ()
            for result in results do
                for k,ts in result do
                    let ok, result = dict.TryGetValue k
                    let aggregator =
                        if ok then result
                        else
                            let agg = new ResizeArray<'T[]> ()
                            dict.Add(k, agg)
                            agg

                    aggregator.Add ts

            return dict |> Seq.map (fun kv -> kv.Key, kv.Value |> Array.concat) |> Seq.toArray
        }

        reduceCombine reduce combine source

    /// <summary>
    ///     Distributed sumBy combinator. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined. 
    /// </summary>
    /// <param name="projection">Summand projection function.</param>
    /// <param name="sources">Input data.</param>
    let inline sumBy (projection : 'T -> 'S) (sources : seq<'T>) =
        reduceCombine (fun ts -> cloud0 { return Array.sumBy projection ts })
                        (fun sums -> cloud0 { return Array.sum sums })
                            sources

    /// <summary>
    ///     Distributed Map/Reduce workflow with cluster balancing. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="mapper">Mapper workflow.</param>
    /// <param name="reducer">Reducer workflow.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input source.</param>
    let mapReduceCloud (mapper : 'T -> Cloud0<'R>) (reducer : 'R -> 'R -> Cloud0<'R>)
                        (init : 'R) (source : seq<'T>) : Cloud<'R> =

        foldCloud (fun s t -> cloud0 { let! s' = mapper t in return! reducer s s'})
                reducer init source

    /// <summary>
    ///     Distributed Map/Reduce workflow with cluster balancing. Inputs are balanced across the
    ///     cluster according to multicore capacity then intermediate results are succesively combined.
    /// </summary>
    /// <param name="mapper">Mapper workflow.</param>
    /// <param name="reducer">Reducer workflow.</param>
    /// <param name="init">Initial state and identity element.</param>
    /// <param name="source">Input source.</param>
    let mapReduce (mapper : 'T -> 'R) (reducer : 'R -> 'R -> 'R)
                    (init : 'R) (source : seq<'T>) : Cloud<'R> =

        fold (fun s t -> let s' = mapper t in reducer s s')
                reducer init source

    //
    //  NonDeterministic Parallelism workflows
    //

    /// <summary>
    ///     General-purpose distributed search combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="chooser">Chooser function acting on partition.</param>
    /// <param name="source">Input data.</param>
    let search (chooser : 'T [] -> Cloud0<'S option>) (source : seq<'T>) : Cloud<'S option> =
        let multiCoreSearch (inputs : 'T []) = cloud0 {
            if inputs.Length < 2 then return! chooser inputs 
            else
                let cores = System.Environment.ProcessorCount
                let chunks = Array.splitByPartitionCount cores inputs
                return!
                    chunks
                    |> Array.map chooser
                    |> Cloud0.Choice
        }

        cloud {
            let inputs = Seq.toArray source
            if inputs.Length < 2 then return! chooser inputs
            else
                let! workers = Cloud.GetAvailableWorkers()
                let chunks = WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers inputs
                return!
                    chunks
                    |> Seq.filter (not << Array.isEmpty << snd)
                    |> Seq.map (fun (w,ch) -> multiCoreSearch ch, w)
                    |> Cloud.Choice
        }

    /// <summary>
    ///     Distributed tryPick combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPickCloud (chooser : 'T -> Cloud0<'S option>) (source : seq<'T>) : Cloud<'S option> =
        search (Cloud0.Sequential.tryPick chooser) source

    /// <summary>
    ///     Distributed tryPick combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="chooser">Chooser function.</param>
    /// <param name="source">Input data.</param>
    let tryPick (chooser : 'T -> 'S option) (source : seq<'T>) : Cloud<'S option> =
        search (lift <| Array.tryPick chooser) source

    /// <summary>
    ///     Distributed tryFind combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFindCloud (predicate : 'T -> Cloud0<bool>) (source : seq<'T>) : Cloud<'T option> =
        tryPickCloud (fun t -> cloud0 { let! b = predicate t in return if b then Some t else None }) source

    /// <summary>
    ///     Distributed tryFind combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let tryFind (predicate : 'T -> bool) (source : seq<'T>) : Cloud<'T option> =
        tryPick (fun t -> if predicate t then Some t else None) source

    /// <summary>
    ///     Distributed forall combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let forallCloud (predicate : 'T -> Cloud0<bool>) (source : seq<'T>) : Cloud<bool> = cloud {
        let! result = search (Cloud0.Sequential.tryPick (fun t -> cloud0 { let! b = predicate t in return if b then None else Some () })) source
        return Option.isNone result
    }

    /// <summary>
    ///     Distributed forall combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let forall (predicate : 'T -> bool) (source : seq<'T>) : Cloud<bool> = cloud {
        let! result = search (fun ts -> cloud0 { return if Array.forall predicate ts then None else Some () }) source
        return Option.isNone result
    }

    /// <summary>
    ///     Distributed exists combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let existsCloud (predicate : 'T -> Cloud0<bool>) (source : seq<'T>) : Cloud<bool> = cloud {
        let! result = search (Cloud0.Sequential.tryPick (fun t -> cloud0 { let! b = predicate t in return if b then Some () else None })) source
        return Option.isSome result
    }

    /// <summary>
    ///     Distributed exists combinator. Balances inputs across
    ///     clusters returning immediate result once a positive result is found,
    ///     while actively cancelling all pending computation.
    /// </summary>
    /// <param name="predicate">Predicate function.</param>
    /// <param name="source">Input data.</param>
    let exists (predicate : 'T -> bool) (source : seq<'T>) : Cloud<bool> = cloud {
        let! result = search (fun ts -> cloud0 { return if Array.exists predicate ts then Some () else None }) source
        return Option.isSome result
    }
﻿namespace MBrace.Flow.Internals.Consumers

open System
open System.IO
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Sort =

    // why use both IComparer AND a projection function?

    /// <summary>
    /// 
    /// </summary>
    /// <param name="comparer"></param>
    /// <param name="projection"></param>
    /// <param name="takeCount"></param>
    /// <param name="flow"></param>
    let sortByGen (comparer : IComparer<'Key>) (projection : ExecutionContext -> 'T -> 'Key) (takeCount : int) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) = cloud0 {
            let results = new List<List<'T>>()
            let! ctx = Cloud.GetExecutionContext()
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
              { new Collector<'T, List<'Key[] * 'T []>> with
                member self.DegreeOfParallelism = flow.DegreeOfParallelism
                member self.Iterator() =
                    let list = new List<'T>()
                    results.Add(list)
                    {   Index = ref -1;
                        Func = (fun value -> list.Add(value));
                        Cts = cts }
                member self.Result =
                    let count = results |> Seq.sumBy (fun list -> list.Count)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for list in results do
                        for i = 0 to list.Count - 1 do
                            let value = list.[i]
                            counter <- counter + 1
                            keys.[counter] <- projection ctx value
                            values.[counter] <- value
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    new List<_>(Seq.singleton
                                    (keys.Take(takeCount).ToArray(),
                                     values.Take(takeCount).ToArray())) }
        }

        let sortByComp =
            cloud {
                let! cts = Cloud.CreateCancellationTokenSource()
                let! results = flow.WithEvaluators (collectorf cts) (fun x -> cloud0 { return x }) (fun result -> cloud0 { match result with [||] -> return List() | _ -> return Array.reduce (fun left right -> left.AddRange(right); left) result })
                let result =
                    let count = results |> Seq.sumBy (fun (keys, _) -> keys.Length)
                    let keys = Array.zeroCreate<'Key> count
                    let values = Array.zeroCreate<'T> count
                    let mutable counter = -1
                    for (keys', values') in results do
                        for i = 0 to keys'.Length - 1 do
                            counter <- counter + 1
                            keys.[counter] <- keys'.[i]
                            values.[counter] <- values'.[i]
                    if box comparer <> null || System.Environment.OSVersion.Platform = System.PlatformID.Unix then
                        Array.Sort(keys, values, comparer)
                    else
                        Sort.parallelSort Environment.ProcessorCount keys values

                    values.Take(takeCount).ToArray()
                return result
            }

        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : Cloud0<Collector<'T, 'S>>) (projection : 'S -> Cloud0<'R>) combiner =
                cloud {
                    let! result = sortByComp
                    return! (Array.ToCloudFlow result).WithEvaluators collectorf projection combiner
                }
        }
﻿namespace MBrace.Library

open System
open System.Runtime.Serialization
open System.Collections.Concurrent

open MBrace.Core
open MBrace.Core.Internals

#nowarn "444"

/// A serializable value factory that will be initialized
/// exactly once in each AppDomain(Worker) that consumes it.
/// Distributed equivalent to System.Threading.ThreadLocal<T> type.
[<Sealed; DataContract>]
type DomainLocal<'T> internal (factory : unit -> 'T) =
    // domain local value container
    static let dict = new ConcurrentDictionary<string, 'T> ()

    [<DataMember(Name = "UUID")>]
    let id = mkUUID()

    [<DataMember(Name = "Factory")>]
    let factory = factory

    /// <summary>
    ///     Returns the value initialized in the local Application Domain.
    /// </summary>
    member __.Value : 'T = dict.GetOrAdd(id, fun _ -> factory ())

/// A serializable value factory that will be initialized
/// exactly once in each AppDomain(Worker) that consumes it.
/// Distributed equivalent to System.Threading.ThreadLocal<T> type.
[<Sealed; DataContract>]
type DomainLocalMBrace<'T> internal (factory : Cloud0<'T>) =
    // domain local value container
    static let dict = new ConcurrentDictionary<string, 'T> ()

    [<DataMember(Name = "UUID")>]
    let id = mkUUID()

    [<DataMember(Name = "Factory")>]
    let factory = factory

    /// <summary>
    ///     Returns the value initialized in the local Application Domain.
    /// </summary>
    member __.Value : Cloud0<'T> = cloud0 {
        let! ctx = Cloud.GetExecutionContext()
        return dict.GetOrAdd(id, fun _ -> Cloud.RunSynchronously(factory, ctx.Resources, ctx.CancellationToken))
    }

/// A serializable value factory that will be initialized
/// exactly once in each AppDomain(Worker) that consumes it.
/// Distributed equivalent to System.Threading.ThreadLocal<T> type.
type DomainLocal =

    /// <summary>
    ///     Creates a new DomainLocal entity with supplied factory.
    /// </summary>
    /// <param name="factory">Factory function.</param>
    static member Create(factory : unit -> 'T) = new DomainLocal<'T>(factory)

    /// <summary>
    ///     Creates a new DomainLocal entity with supplied MBrace factory workflow.
    /// </summary>
    /// <param name="factory">Factory workflow.</param>
    static member Create(factory : Cloud0<'T>) = new DomainLocalMBrace<'T>(factory)
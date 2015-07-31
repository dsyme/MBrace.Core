﻿namespace MBrace.Core.Tests

open System.Collections.Generic

open MBrace.Core
open MBrace.Library

type DummyDisposable() =
    let isDisposed = ref false
    interface ICloudDisposable with
        member __.Dispose () = async { isDisposed := true }

    member __.IsDisposed = !isDisposed

type CloudTree<'T> = Leaf | Branch of 'T * TreeRef<'T> * TreeRef<'T>

and TreeRef<'T> = PersistedValue<CloudTree<'T>>

module CloudTree =

    let rec createTree d = cloud {
        if d = 0 then return! PersistedValue.New Leaf
        else
            let! l,r = createTree (d-1) <||> createTree (d-1)
            return! PersistedValue.New (Branch(d, l, r))
    }

    let rec getBranchCount (tree : TreeRef<int>) = cloud {
        let! value = tree.GetValueAsync()
        match value with
        | Leaf -> return 0
        | Branch(_,l,r) ->
            let! c,c' = getBranchCount l <||> getBranchCount r
            return 1 + c + c'
    }

module WordCount =

    let run size mapReduceAlgorithm : Cloud<int> =
        let mapF (text : string) = cloud0 { return text.Split(' ').Length }
        let reduceF i i' = cloud0 { return i + i' }
        let inputs = Array.init size (fun i -> "lorem ipsum dolor sit amet")
        mapReduceAlgorithm mapF reduceF 0 inputs

    // naive, binary recursive mapreduce implementation
    let rec mapReduceRec (mapF : 'T -> Cloud0<'S>) 
                            (reduceF : 'S -> 'S -> Cloud0<'S>) 
                            (id : 'S) (inputs : 'T []) =
        cloud {
            match inputs with
            | [||] -> return id
            | [|t|] -> return! mapF t
            | _ ->
                let left = inputs.[.. inputs.Length / 2 - 1]
                let right = inputs.[inputs.Length / 2 ..]
                let! s,s' = (mapReduceRec mapF reduceF id left) <||> (mapReduceRec mapF reduceF id right)
                return! reduceF s s'
        }
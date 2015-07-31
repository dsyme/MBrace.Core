﻿namespace MBrace.Core.Tests

#nowarn "0444" // Disable mbrace warnings
open System
open System.Linq
open System.Collections.Generic
open System.Net
open System.IO
open FsCheck
open NUnit.Framework
open MBrace.Flow
open MBrace.Flow.Internals
open MBrace.Core
open System.Text

// Helper type
type Separator = N | R | RN

// General-purpose, runtime independent CloudFlow tests
[<TestFixture>]
module ``CloudFlow Core property tests`` =

    [<Test>]
    let ``ResizeArray concatenator`` () =
        let check (inputs : int[][]) =
            let tas = inputs |> Array.map (fun ts -> new ResizeArray<_>(ts))
            let expected = inputs |> Array.concat
            ResizeArray.concat tas |> Seq.toArray |> shouldEqual expected

        Check.QuickThrowOnFail check

    [<Test>]
    let ``Partition by size`` () =
        let check (inputs : int64 [], maxSize : int64) =
            let maxSize = 1L + abs maxSize
            let partitions =
                inputs
                |> Partition.partitionBySize (fun i -> async { return abs i }) maxSize
                |> Async.RunSynchronously
        
            Array.concat partitions |> shouldEqual inputs

            for p in partitions do
                if Array.sum p > maxSize then
                    Array.sum p.[0 .. p.Length - 2] |> shouldBe (fun s -> s <= maxSize)

        Check.QuickThrowOnFail check


[<TestFixture; AbstractClass>]
type ``CloudFlow tests`` () as self =
    let runRemote (workflow : Cloud<'T>) = self.RunRemote(workflow)
    let runLocally (workflow : Cloud<'T>) = self.RunLocally(workflow)

    /// Urls for running HTTP tests
    let testUrls = 
        [| 
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-antony-23.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-as-12.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-comedy-7.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-coriolanus-24.txt";
            "http://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt" 
        |]

    let getHttpFileLineCount (url : string) = 
        let mutable lineCount = 0L
        let client = new WebClient()
        use stream = client.OpenRead(url)
        use reader = new StreamReader(stream)

        while reader.ReadLine() <> null do
            lineCount <- lineCount + 1L

        lineCount

    abstract RunRemote : Cloud<'T> -> 'T
    abstract RunLocally : Cloud<'T> -> 'T
    abstract IsSupportedStorageLevel : StorageLevel -> bool
    abstract FsCheckMaxNumberOfTests : int
    abstract FsCheckMaxNumberOfIOBoundTests : int

    // #region Flow persist tests

    [<Test>]
    member __.``1. PersistedCloudFlow : Simple StorageLevel.Disk`` () =
        if __.IsSupportedStorageLevel StorageLevel.Disk then
            let inputs = [|1L .. 1000000L|]
            let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.Disk |> runRemote
            let workers = Cloud.GetWorkerCount() |> runRemote
            persisted.StorageLevel |> shouldEqual StorageLevel.Disk
            persisted.PartitionCount |> shouldEqual workers
            persisted |> CloudFlow.toArray |> runRemote |> shouldEqual inputs
            persisted |> Seq.toArray |> shouldEqual inputs

    [<Test>]
    member __.``1. PersistedCloudFlow : Randomized StorageLevel.Disk`` () =
        if __.IsSupportedStorageLevel StorageLevel.Disk then
            let f(xs : int[]) =            
                let x = xs |> CloudFlow.OfArray |> CloudFlow.map ((+)1) |> CloudFlow.persist StorageLevel.Disk |> runRemote
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                Assert.AreEqual(y, x |> Seq.toArray)
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``1. PersistedCloudFlow : StorageLevel.Memory`` () =
        if __.IsSupportedStorageLevel StorageLevel.Memory then
            let inputs = [|1L .. 1000000L|]
            let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.cache |> runRemote
            let workers = Cloud.GetWorkerCount() |> runRemote
            persisted.StorageLevel |> shouldEqual StorageLevel.Memory
            persisted.PartitionCount |> shouldEqual workers
            persisted |> CloudFlow.toArray |> runRemote |> shouldEqual inputs

    [<Test>]
    member __.``1. PersistedCloudFlow : disposal`` () =
        let inputs = [|1 .. 1000000|]
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.Disk |> runRemote
        persisted |> Cloud.Dispose |> runRemote
        shouldfail(fun () -> persisted |> CloudFlow.length |> runRemote |> ignore)

    [<Test>]
    member __.``1. PersistedCloudFlow : StorageLevel.Memory caching`` () =
        if __.IsSupportedStorageLevel StorageLevel.Memory then
            // ensure that initial array is large enough for disk caching to kick in,
            // otherwise persisted fragments will be encapsulated in the reference as an optimization.
            // Limit is currently set to 512KiB, so total size must be 512KiB * cluster size.
            // Arrays have reference equality semantics, compare hashcodes to ensure that caching is taking place.
            let cv = [|1 .. 20|] |> CloudFlow.OfArray |> CloudFlow.map (fun _ -> [|1L .. 1000000L|]) |> CloudFlow.cache |> runRemote
            let x = cv |> CloudFlow.map (fun ref -> ref.GetHashCode()) |> CloudFlow.toArray |> runRemote
            for i in 1 .. 5 do
                let y = cv |> CloudFlow.map (fun ref -> ref.GetHashCode()) |> CloudFlow.toArray |> runRemote
                Assert.AreEqual(x, y)

    [<Test>]
    member __.``1. PersistedCloudFlow : Simple StorageLevel.MemorySerialized`` () =
        if __.IsSupportedStorageLevel StorageLevel.MemorySerialized then
            let f(xs : int[]) =            
                let pf = xs |> CloudFlow.OfArray |> CloudFlow.map ((+)1) |> CloudFlow.persist StorageLevel.MemorySerialized |> runRemote
                let ys = pf |> CloudFlow.length |> runRemote
                Assert.AreEqual(ys, xs.LongLength)
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``1. PersistedCloudFlow : merging`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.MemoryAndDisk |> runRemote
        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> persisted))
        merged.PartitionCount |> shouldEqual (N * persisted.PartitionCount)
        
        let partitions = persisted.GetPartitions() |> Array.map snd
        let mergedPartitions = merged.GetPartitions() |> Array.map snd
        
        mergedPartitions
        |> Seq.countBy (fun p -> p.Id)
        |> Seq.forall (fun (_,n) -> n = N)
        |> shouldEqual true

        for i = 0 to mergedPartitions.Length - 1 do
            mergedPartitions.[i].Id |> shouldEqual (partitions.[i % persisted.PartitionCount].Id)

        merged
        |> Seq.toArray
        |> shouldEqual (Array.init N (fun _ -> inputs) |> Array.concat)

    [<Test>]
    member __.``1. PersistedCloudFlow : merged disposal`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.MemoryAndDisk |> runRemote
        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> persisted))
        merged |> Cloud.Dispose |> runRemote
        shouldfail(fun () -> persisted |> Seq.iter ignore)

    // #region Streams tests

    [<Test>]
    member __.``2. CloudFlow : ofArray`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.length |> runRemote
            let y = xs |> Seq.map ((+)1) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofSeqs`` () =
        let tester (xs : int [] []) =
            let flowResult =
                xs
                |> CloudFlow.OfSeqs
                |> CloudFlow.map (fun x -> x * x)
                |> CloudFlow.sum
                |> runRemote

            let seqResult =
                xs
                |> Seq.concat
                |> Seq.map (fun x -> x * x)
                |> Seq.sum

            Assert.AreEqual(seqResult, flowResult)

        Check.QuickThrowOnFail(tester, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with custom deserializer`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> cloud0 { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runLocally

            let paths = cfs |> Array.map (fun cf -> cf.Path)
            let x =     CloudFlow.OfCloudFiles(paths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length })) 
                        |> CloudFlow.length
                        |> runRemote

            Assert.AreEqual(paths.Length, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with small threshold`` () =
        let path = CloudPath.GetRandomFileName() |> runLocally
        let file = CloudFile.WriteAllText(path, "Lorem ipsum dolor sit amet") |> runLocally
        let f (size : int) =
            let size = abs size % 200
            let repeatedPaths = Array.init size (fun _ -> file.Path)
            let length =
                CloudFlow.OfCloudFiles(repeatedPaths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length }), sizeThresholdPerCore = 1L)
                |> CloudFlow.length
                |> runRemote

            Assert.AreEqual(int64 size, length)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> cloud0 { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runLocally

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runRemote
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFilesByLine with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> cloud0 { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runLocally

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runRemote
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)
    
    
    [<Test>]
    member __.``2. CloudFlow : OfCloudFileByLine`` () =
        
        let f(xs : string [], separator : Separator) =
            let separator = 
                match separator with
                | N -> "\n" 
                | R -> "\r"
                | RN -> "\r\n"

            let path = CloudPath.GetRandomFileName() |> runLocally
            let cf = CloudFile.WriteAllText(path, xs |> String.concat separator) |> runLocally
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.OfCloudFileByLine
                |> CloudFlow.toArray
                |> runRemote
                |> Array.sortBy id
                    
            
            let y = 
                __.RunLocally(cloud { return! CloudFile.ReadLines cf.Path })
                |> Seq.sortBy id
                |> Seq.toArray
                    
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)


    [<Test>]
    member __.``2. CloudFlow : OfCloudFileByLine with Big TextFile `` () =
        
        let f(count : int, separator : Separator) =
            let separator = 
                match separator with
                | N -> "\n" 
                | R -> "\r"
                | RN -> "\r\n"

            let path = CloudPath.GetRandomFileName() |> runLocally
            let cf = CloudFile.WriteAllText(path, [|1..(Math.Abs(count) * 1000)|] |> Array.map string |> String.concat separator) |> runLocally
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.OfCloudFileByLine
                |> CloudFlow.length
                |> runRemote
                
            let y = 
                __.RunLocally(cloud { let! lines = CloudFile.ReadLines path in return lines |> Seq.length })
                            
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadAllLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> cloud0 { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runLocally

            let x = cfs 
                        |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runRemote
                        |> Set.ofArray

            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : OfHttpFileByLine single input`` () =
        if not isAppVeyorInstance then
            for url in testUrls do
                let lineCount = getHttpFileLineCount url

                let flowLength = 
                    CloudFlow.OfHttpFileByLine url
                    |> CloudFlow.length
                    |> runRemote
                            
                Assert.AreEqual(lineCount, flowLength)

    [<Test>]
    member __.``2. CloudFlow : OfHttpFileByLine multiple inputs`` () =
        if not isAppVeyorInstance then
            let lineCount = testUrls |> Array.Parallel.map getHttpFileLineCount |> Array.sum

            let flowLength =
                CloudFlow.OfHttpFileByLine testUrls
                |> CloudFlow.length
                |> runRemote

            Assert.AreEqual(lineCount, flowLength)

    [<Test>]
    member __.``2. CloudFlow : map`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : filter`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : choose`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.choose (fun n -> if n % 2 = 0 then Some(string n) else None) |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.choose (fun n -> if n % 2 = 0 then Some(string n) else None) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : collect`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.collect (fun n -> [|1..n|]) |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : fold`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.fold (+) (+) (fun () -> 0) |> runRemote
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : sum`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.sum |> runRemote
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : length`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.length |> runRemote
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : countBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.countBy id |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
            Assert.AreEqual(set y, set x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : sortBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray |> runRemote
            let y = (xs |> Seq.sortBy id).Take(10).ToArray()
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : maxBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id |> runRemote in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id  |> runRemote
                let y = xs |> Seq.maxBy id
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : minBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.minBy id |> runRemote in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.minBy id  |> runRemote
                let y = xs |> Seq.minBy id
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : reduce`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> runRemote in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> runRemote
                let y = xs |> Seq.reduce (+)
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : reduceBy`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.reduceBy id (+) |> CloudFlow.toArray |> runRemote
            let y = xs |> Seq.groupBy id |> Seq.map (fun (k, vs) -> k, vs |> Seq.reduce (+)) |> Seq.toArray
            Assert.AreEqual(Array.sortBy fst y, Array.sortBy fst x)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : averageBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> runRemote in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> runRemote
                let y = xs |> Seq.averageBy (float)
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : groupBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.groupBy id
                |> CloudFlow.map (fun (k, xs) -> k, Seq.length xs)
                |> CloudFlow.toArray
                |> runRemote
            let y =
                xs
                |> Seq.groupBy id
                |> Seq.map (fun (k, xs) -> k, Seq.length xs)
                |> Seq.toArray
            (x |> Array.sortBy fst) = (y |> Array.sortBy fst)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : distinctBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> runRemote
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : distinct`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> runRemote
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : take`` () =
        let f (xs : int[], n : int) =
            let n = System.Math.Abs(n)
            let x = xs |> CloudFlow.OfArray |> CloudFlow.take n |> CloudFlow.toArray |> runRemote
            Assert.AreEqual(min xs.Length n, x.Length)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : withDegreeOfParallelism`` () =
        let f(xs : int[]) = 
            let r = xs 
                    |> CloudFlow.OfArray
                    |> CloudFlow.map (fun _ -> System.Diagnostics.Process.GetCurrentProcess().Id)
                    |> CloudFlow.withDegreeOfParallelism 1
                    |> CloudFlow.toArray
                    |> runRemote
            let x = r
                    |> Set.ofArray
                    |> Seq.length
            if xs.Length = 0 then x = 0
            else x = 1
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : tryFind`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryFind (fun n -> n = 0) |> runRemote
            let y = xs |> Seq.tryFind (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : find`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.find (fun n -> n = 0) |> runRemote with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.find (fun n -> n = 0) with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : tryPick`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryPick (fun n -> if n = 0 then Some n else None) |> runRemote
            let y = xs |> Seq.tryPick (fun n -> if n = 0 then Some n else None) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : pick`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.pick (fun n -> if n = 0 then Some n else None) |> runRemote with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : exists`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.exists (fun n -> n = 0) |> runRemote
            let y = xs |> Seq.exists (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : forall`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.forall (fun n -> n = 0) |> runRemote
            let y = xs |> Seq.forall (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : isEmpty`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.isEmpty |> runRemote
            let y = xs |> Seq.isEmpty
            x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudQueue`` () =
        let f(_ : int) =
            let x =
                cloud {
                    let! queue = CloudQueue.New()
                    let! n =
                        Cloud.Choice [
                            cloud { 
                                for i in [|1..1000|] do
                                    do! CloudQueue.Enqueue(queue, i)
                                    do! Cloud.Sleep(100)
                                return None
                            };
                            cloud {
                                let! n =  
                                    CloudFlow.OfCloudQueue(queue, 1)
                                    |> CloudFlow.take 2
                                    |> CloudFlow.length
                                return Some n
                            }]
                    return Option.get n
                }
                |> runRemote
            x = 2L
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)


    [<Test>]
    member __.``2. CloudFlow : toCloudQueue`` () =
        let f(xs : int[]) =
            let queue = CloudQueue.New() |> runRemote
            let x = 
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> v + 1)
                |> CloudFlow.toCloudQueue queue
                |> runRemote
            let x = 
                cloud {
                    let list = ResizeArray<int>()
                    for x in xs do 
                        let! v = CloudQueue.Dequeue(queue)
                        list.Add(v)
                    return list
                } |> runRemote
            let y = xs |> Seq.map (fun v -> v + 1) |> Seq.toArray
            (set x) = (set y)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)


    [<Test>]
    member __.``2. CloudFlow : toTextCloudFiles`` () =
        let f(xs : int[]) =
            let xs = xs |> Array.map string
            let dir = CloudPath.GetRandomDirectoryName() |> __.RunLocally
            let cfs = 
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.toTextCloudFiles dir
                |> runRemote
            let ys = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Seq.toArray
            Assert.AreEqual(xs, ys)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

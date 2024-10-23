﻿namespace MBrace.FsPickler

open System

open System
open System.IO
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text

open MBrace.FsPickler.Utils
open MBrace.FsPickler.Hashing
open MBrace.FsPickler.ReflectionCache
open MBrace.FsPickler.RootSerialization

/// <summary>
///     An abstract class containg the basic serialization API.
/// </summary>
[<AbstractClass>]
type FsPicklerSerializer (formatProvider : IPickleFormatProvider, [<O;D(null)>]?typeConverter : ITypeNameConverter, [<O;D(null)>]?picklerResolver : IPicklerResolver) =

    let resolver = defaultArg picklerResolver (PicklerCache.Instance :> IPicklerResolver)
    let reflectionCache = ReflectionCache.Create(?tyConv = typeConverter)

    member __.Resolver = resolver
    member internal __.ReflectionCache = reflectionCache

    /// Declares that dynamic subtype resolution should be disabled during serialization.
    /// This explicitly prohibits serialization/deserialization of any objects whose type
    /// is specified in the serialization payload. Examples of such types are System.Object,
    /// F# functions and delegates. Defaults to false.
    member val DisableSubtypeResolution = false with get,set

    /// Declares that FsPickler should make no attempt of its own to load Assemblies
    /// that are specified in the serialization format. Will result in a deserialization
    /// exception if required assembly is missing from the current AppDomain. Defaults to false.
    member val DisableAssemblyLoading = false with get,set

    /// <summary>
    ///     Description of the pickle format used by the serializer.
    /// </summary>
    member __.PickleFormat = formatProvider.Name

    //
    //  Typed API
    //

    /// <summary>Serialize value to the underlying stream.</summary>
    /// <param name="stream">Target write stream.</param>
    /// <param name="value">Value to be serialized.</param>
    /// <param name="pickler">Pickler used for serialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    member __.Serialize<'T>(stream : Stream, value : 'T, 
                                [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext,
                                [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : unit =
        let tt = typeof<'T>
        let pickler = match pickler with None -> resolver.Resolve<'T> () | Some p -> p
        use writer = initStreamWriter formatProvider stream encoding false leaveOpen
        let _ = writeRootObject resolver reflectionCache writer streamingContext None false __.DisableSubtypeResolution pickler value
        ()

    /// <summary>Deserialize value of given type from the underlying stream.</summary>
    /// <param name="stream">Source read stream.</param>
    /// <param name="pickler">Pickler used for deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    member __.Deserialize<'T> (stream : Stream, [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : 'T =

        let pickler = match pickler with None -> resolver.Resolve<'T> () | Some p -> p
        use reader = initStreamReader formatProvider stream encoding false leaveOpen
        readRootObject resolver reflectionCache reader streamingContext None __.DisableSubtypeResolution __.DisableAssemblyLoading pickler

    /// <summary>Serialize a sequence of objects to the underlying stream.</summary>
    /// <param name="stream">Target write stream.</param>
    /// <param name="sequence">Input sequence to be evaluated and serialized.</param>
    /// <param name="pickler">Pickler used for serialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    /// <return>Number of elements written to the stream.</return>
    member __.SerializeSequence<'T>(stream : Stream, sequence:seq<'T>, [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                                        [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : int =

        let pickler = match pickler with None -> resolver.Resolve<'T> () | Some p -> p
        use writer = initStreamWriter formatProvider stream encoding true leaveOpen
        writeTopLevelSequence resolver reflectionCache writer streamingContext false __.DisableSubtypeResolution pickler sequence


    /// <summary>Lazily deserialize a sequence of objects from the underlying stream.</summary>
    /// <param name="stream">Source read stream.</param>
    /// <param name="pickler">Pickler used for element deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    /// <returns>An IEnumerable that lazily consumes elements from the stream.</returns>
    member __.DeserializeSequence<'T>(stream : Stream, [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                        [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : seq<'T> =

        let pickler = match pickler with None -> resolver.Resolve<'T>() | Some p -> p
        let reader = initStreamReader formatProvider stream encoding true leaveOpen
        readTopLevelSequence resolver reflectionCache reader streamingContext __.DisableSubtypeResolution __.DisableAssemblyLoading pickler

    /// <summary>
    ///     Serializes a value to stream, excluding objects mandated by the provided IObjectSifter instance.
    ///     Values excluded from serialization will be returned tagged by their ids. 
    ///     Sifted objects will have to be provided on deserialization along with their accompanying id's.
    /// </summary>
    /// <param name="stream">Target write stream.</param>
    /// <param name="value">Value to be serialized.</param>
    /// <param name="sifter">User supplied sifter implementation. Used to specify which nodes in the object graph are to be excluded from serialization.</param>
    /// <param name="pickler">Pickler used for element deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    /// <returns>Sifted values along with their graph ids.</returns>
    member __.SerializeSifted<'T>(stream : Stream, value:'T, sifter : IObjectSifter, 
                                                    [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                    [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : (int64 * obj) [] =

        let pickler = match pickler with None -> resolver.Resolve<'T>() | Some p -> p
        use writer = initStreamWriter formatProvider stream encoding false leaveOpen
        let state = writeRootObject resolver reflectionCache writer streamingContext (Some sifter) false __.DisableSubtypeResolution pickler value
        state.Sifted.ToArray()

    /// <summary>
    ///     Deserializes a sifted value from stream, filling in sifted holes from the serialized using supplied objects.
    /// </summary>
    /// <param name="stream">Source read stream.</param>
    /// <param name="sifted">Object-id pairs used for filling sifted holes in serialization.s</param>
    /// <param name="pickler">Pickler used for element deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    member __.DeserializeSifted<'T>(stream : Stream, sifted : (int64 * obj) [],
                                                    [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                    [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : 'T =

        let pickler = match pickler with None -> resolver.Resolve<'T> () | Some p -> p
        use reader = initStreamReader formatProvider stream encoding false leaveOpen
        readRootObject resolver reflectionCache reader streamingContext (Some sifted) __.DisableSubtypeResolution __.DisableAssemblyLoading pickler

    /// <summary>
    ///     Pickles given value to byte array.
    /// </summary>
    /// <param name="value">Value to pickle.</param>
    /// <param name="pickler">Pickler used for element serialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    member bp.Pickle<'T>(value : 'T, [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, [<O;D(null)>]?encoding : Encoding) : byte [] =
        pickleBinary (fun m v -> bp.Serialize(m, v, ?pickler = pickler, ?streamingContext = streamingContext, ?encoding = encoding)) value


    /// <summary>
    ///     Unpickles value using given pickler.
    /// </summary>
    /// <param name="data">Pickle to deserialize.</param>
    /// <param name="pickler">Pickler used for element serialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    member bp.UnPickle<'T> (data : byte [], [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, [<O;D(null)>]?encoding : Encoding) : 'T =
        unpickleBinary (fun m -> bp.Deserialize(m, ?pickler = pickler, ?streamingContext = streamingContext, ?encoding = encoding)) data


    /// <summary>
    ///     Pickles value to bytes, excluding objects mandated by the provided IObjectSifter instance.
    ///     Values excluded from serialization will be returned tagged by their ids. 
    ///    Sifted objects will have to be provided on deserialization along with their accompanying id's.
    /// </summary>
    /// <param name="value">Value to be serialized.</param>
    /// <param name="sifter">User supplied sifter implementation. Used to specify which nodes in the object graph are to be excluded from serialization.</param>
    /// <param name="pickler">Pickler used for element deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <returns>Pickled value along with sifted values along with their graph ids.</returns>
    member bp.PickleSifted<'T>(value:'T, sifter : IObjectSifter, 
                                                    [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                    [<O;D(null)>]?encoding : Encoding) : byte [] * (int64 * obj) [] =
        use m = new MemoryStream()
        let sifted = bp.SerializeSifted(m, value, sifter, ?pickler = pickler, ?streamingContext = streamingContext, ?encoding = encoding)
        m.ToArray(), sifted

    /// <summary>
    ///     Unpickles a sifted value, filling in sifted holes from the serialized using supplied objects.
    /// </summary>
    /// <param name="pickle">Pickle to deserialize.</param>
    /// <param name="sifted">Object-id pairs used for filling sifted holes in serialization.</param>
    /// <param name="pickler">Pickler used for element deserialization. Defaults to auto-generated pickler.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    member bp.UnPickleSifted<'T>(pickle : byte[], sifted : (int64 * obj) [],
                                                    [<O;D(null)>]?pickler : Pickler<'T>, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                    [<O;D(null)>]?encoding : Encoding) : 'T =
        use m = new MemoryStream(pickle)
        bp.DeserializeSifted<'T>(m, sifted, ?pickler = pickler, ?streamingContext = streamingContext, ?encoding = encoding)


    //
    //  Untyped API
    //


    /// <summary>Serialize untyped object to the underlying stream with provided pickler.</summary>
    /// <param name="stream">Target write stream.</param>
    /// <param name="value">Value to be serialized.</param>
    /// <param name="pickler">Untyped pickler used for serialization. Its type should be compatible with that of the supplied object.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    member __.SerializeUntyped(stream : Stream, value : obj, pickler : Pickler, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : unit =
        use writer = initStreamWriter formatProvider stream encoding false leaveOpen
        let _ = writeRootObjectUntyped resolver reflectionCache writer streamingContext None false __.DisableSubtypeResolution pickler value
        ()

    /// <summary>Deserialize untyped object from the underlying stream with provided pickler.</summary>
    /// <param name="stream">Source read stream.</param>
    /// <param name="pickler">Pickler used for deserialization. Its type should be compatible with that of the supplied object.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    member __.DeserializeUntyped(stream : Stream, pickler : Pickler, [<O;D(null)>]?streamingContext : StreamingContext, 
                                                                [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : obj =

        use reader = initStreamReader formatProvider stream encoding false leaveOpen
        readRootObjectUntyped resolver reflectionCache reader streamingContext None __.DisableSubtypeResolution __.DisableAssemblyLoading pickler

    /// <summary>Serialize an untyped sequence of objects to the underlying stream.</summary>
    
    /// <param name="stream">Target write stream.</param>
    /// <param name="sequence">Input sequence to be evaluated and serialized.</param>
    /// <param name="pickler">Pickler used for element serialization. Its type should be compatible with that of the supplied sequence.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    /// <return>Number of elements written to the stream.</return>
    member __.SerializeSequenceUntyped(stream : Stream, sequence : IEnumerable, pickler : Pickler, 
                                        [<O;D(null)>]?streamingContext : StreamingContext, [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : int =

        use writer = initStreamWriter formatProvider stream encoding true leaveOpen
        writeTopLevelSequenceUntyped resolver reflectionCache writer streamingContext false __.DisableSubtypeResolution pickler sequence

    /// <summary>Lazily deserialize an untyped sequence of objects from the underlying stream.</summary>
    /// <param name="stream">source stream.</param>
    /// <param name="pickler">Pickler used for element deserialization. Its type should be compatible with that of the supplied sequence.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    /// <param name="leaveOpen">Leave underlying stream open when finished. Defaults to false.</param>
    /// <returns>An IEnumerable that lazily consumes elements from the stream.</returns>
    member __.DeserializeSequenceUntyped(stream : Stream, pickler : Pickler, [<O;D(null)>]?streamingContext : StreamingContext, 
                                            [<O;D(null)>]?encoding : Encoding, [<O;D(null)>]?leaveOpen : bool) : IEnumerable =

        let reader = initStreamReader formatProvider stream encoding true leaveOpen
        readTopLevelSequenceUntyped resolver reflectionCache reader streamingContext __.DisableSubtypeResolution __.DisableAssemblyLoading pickler

    /// <summary>
    ///     Pickles given value to byte array.
    /// </summary>
    /// <param name="value">Value to pickle.</param>
    /// <param name="pickler">Pickler used for value serialization. Its type should be compatible with that of the supplied value.</param>
    /// <param name="streamingContext">Streaming context for serialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    member __.PickleUntyped (value : obj, pickler : Pickler, [<O;D(null)>]?streamingContext : StreamingContext, [<O;D(null)>]?encoding : Encoding) : byte [] =
        pickleBinary (fun m v -> __.SerializeUntyped(m, v, pickler, ?streamingContext = streamingContext, ?encoding = encoding)) value


    /// <summary>
    ///     Unpickle value to given type.
    /// </summary>
    /// <param name="pickle">Byte array to unpickler.</param>
    /// <param name="pickler">Pickler used for value serialization. Its type should be compatible with that of the supplied pickle.</param>
    /// <param name="streamingContext">Streaming context for deserialization state. Defaults to the empty streaming context.</param>
    /// <param name="encoding">Text encoding used by the deserializer.</param>
    member __.UnPickleUntyped (pickle : byte [], pickler : Pickler, [<O;D(null)>]?streamingContext : StreamingContext, [<O;D(null)>]?encoding : Encoding) : obj =
        unpickleBinary (fun m -> __.DeserializeUntyped(m, pickler, ?streamingContext = streamingContext, ?encoding = encoding)) pickle

    
    //
    //  Misc tools
    //

    /// <summary>Compute size and hashcode for given input.</summary>
    /// <param name="value">input value.</param>
    /// <param name="hashFactory">the hashing algorithm to be used. MurMur3 by default.</param>
    member __.ComputeHash<'T>(value : 'T, [<O;D(null)>] ?hashFactory : IHashStreamFactory) =
        let signature = reflectionCache.GetTypeSignature (if obj.ReferenceEquals(value,null) then typeof<obj> else value.GetType())
        let hashStream = 
            match hashFactory with 
            | Some h -> h.Create()
            | None -> new MurMur3Stream() :> HashStream

        do
            use writer = initStreamWriter formatProvider hashStream None false None
            let pickler = resolver.Resolve<obj>()
            let _ = writeRootObject resolver reflectionCache writer None None true false pickler (box value)
            ()

        {
            Algorithm = hashStream.HashAlgorithm
            Type = signature
            Length = hashStream.Length
            Hash = hashStream.ComputeHash()
        }

    /// <summary>Compute size in bytes for given input.</summary>
    /// <param name="value">input value.</param>
    /// <param name="pickler">Pickler to be used for size computation. Defaults to auto-generated pickler.</param>
    member __.ComputeSize<'T>(value : 'T, [<O;D(null)>] ?pickler : Pickler<'T>) =
        let pickler = match pickler with Some p -> p | None -> resolver.Resolve<'T> ()
        let lengthCounter = new LengthCounterStream()
        do
            use writer = initStreamWriter formatProvider lengthCounter None false None
            let _ = writeRootObject resolver reflectionCache writer None None true false pickler value
            ()

        lengthCounter.Count

    /// <summary>
    ///     Creates a state object used for computing accumulated sizes for multiple objects.
    /// </summary>
    /// <param name="encoding">Text encoding used by the serializer.</param>
    /// <param name="resetInterval">Specifies the serialized object interval after which serialization state will be reset. Defaults to no interval.</param>
    member bp.CreateObjectSizeCounter([<O;D(null)>] ?encoding : Encoding, ?resetInterval:int64) : ObjectSizeCounter =
        new ObjectSizeCounter(formatProvider, resolver, reflectionCache, encoding, resetInterval)
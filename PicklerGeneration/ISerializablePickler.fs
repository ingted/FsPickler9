﻿namespace MBrace.FsPickler

open System
open System.IO
open System.Reflection
open System.Threading
open System.Runtime.Serialization

open MBrace.FsPickler
open MBrace.FsPickler.Reflection

#if EMIT_IL
open System.Reflection.Emit
open MBrace.FsPickler.Emit
open MBrace.FsPickler.PicklerEmit
#endif

[<AutoOpen>]
module private ISerializableUtils =

    type private ExceptionInfoHolder(si : SerializationInfo, sc : StreamingContext) =
        inherit Exception(si, sc)

    let mkDummyException si sc = new ExceptionInfoHolder(si, sc) :> Exception

    let inline mkSerializationInfo<'T> () = new SerializationInfo(typeof<'T>, new FormatterConverter())

    let inline checkSerializationSubtype (w : WriteState) (value : 'T) =
        if w.DisableSubtypeResolution then
            let msg = sprintf "Subtype serialization has been disabled. Value %A:%O is ISerializable." value typeof<'T>
            raise <| new FsPicklerException(msg)

    let inline checkDeserializationSubtype<'T> (r : ReadState) =
        if r.DisableSubtypeResolution then
            let msg = sprintf "Subtype deserialization has been disabled. Type %O is ISerializable." typeof<'T>
            raise <| new FsPicklerException(msg)

    let inline resolveEntryPickler (r : IPicklerResolver) (entry : SerializationEntry) =
        let picklerType = match entry.Value with null -> entry.ObjectType | o -> o.GetType()
        r.Resolve picklerType

    let inline writeSerializationEntry (w : WriteState) (entry : SerializationEntry) =
        let formatter = w.Formatter
        let op = resolveEntryPickler w.PicklerResolver entry
        formatter.BeginWriteObject "entry" ObjectFlags.None
        formatter.WriteString "Name" entry.Name
        w.TypePickler.Write w "Type" op.Type
        op.UntypedWrite w "Value" entry.Value
        formatter.EndWriteObject()

    let inline readSerializationEntry (r : ReadState) (sI : SerializationInfo) =
        let formatter = r.Formatter
        let _ = formatter.BeginReadObject "entry"
        let name = formatter.ReadString "Name"
        let objectType = r.TypePickler.Read r "Type"
        let op = r.PicklerResolver.Resolve objectType
        let value = op.UntypedRead r "Value"
        sI.AddValue(name, value)
        formatter.EndReadObject()

    let inline writeSerializationInfo (w : WriteState) (sI : SerializationInfo) =
        let enum = sI.GetEnumerator()
        let formatter = w.Formatter

        if formatter.PreferLengthPrefixInSequences then
            formatter.WriteInt32 "memberCount" sI.MemberCount
            formatter.BeginWriteObject "serializationEntries" ObjectFlags.IsSequenceHeader

            while enum.MoveNext() do
                writeSerializationEntry w enum.Current

            formatter.EndWriteObject ()

        else
            formatter.BeginWriteObject "serializationEntries" ObjectFlags.IsSequenceHeader

            while enum.MoveNext() do
                formatter.WriteNextSequenceElement true
                writeSerializationEntry w enum.Current

            formatter.WriteNextSequenceElement false
            formatter.EndWriteObject ()

    let inline readSerializationInfo<'T> (r : ReadState) =
        let sI = mkSerializationInfo<'T> ()
        let formatter = r.Formatter

        if formatter.PreferLengthPrefixInSequences then
            let memberCount = formatter.ReadInt32 "memberCount"
            let _ = formatter.BeginReadObject "serializationEntries"
            for i = 1 to memberCount do
                readSerializationEntry r sI
            formatter.EndReadObject ()
        else
            let _ = formatter.BeginReadObject "serializationEntries"
            while formatter.ReadNextSequenceElement() do
                readSerializationEntry r sI
            formatter.EndReadObject ()

        sI

    let inline cloneSerializationInfo<'T> (c : CloneState) (sI : SerializationInfo) =
        let sI' = new SerializationInfo(sI.ObjectType, new FormatterConverter())
        let enum = sI.GetEnumerator()
        while enum.MoveNext() do
            let se = enum.Current
            let ep = resolveEntryPickler c.PicklerResolver se
            let o = ep.UntypedClone c se.Value
            sI'.AddValue(se.Name, o)

        sI'

    let inline acceptSerializationInfo (v : VisitState) (sI : SerializationInfo) =
        let enum = sI.GetEnumerator()
        while enum.MoveNext() do
            let se = enum.Current
            let ep = resolveEntryPickler v.PicklerResolver se
            ep.UntypedAccept v se.Value

type internal ISerializablePickler =

    static member Create<'T when 'T :> ISerializable>() =
        let ctorInfo = 
            match typeof<'T>.TryGetConstructor [| typeof<SerializationInfo> ; typeof<StreamingContext> |] with
            | Some ctor -> ctor
            | None -> invalidArg "T" <| sprintf "Type '%O' missing a (SerializationInfo, StreamingContext) constructor." typeof<'T>

        let allMethods = typeof<'T>.GetMethods(allMembers)
        let onSerializing = allMethods |> getSerializationMethods<OnSerializingAttribute> |> wrapDelegate<Action<'T, StreamingContext>>
        let onSerialized = allMethods |> getSerializationMethods<OnSerializedAttribute> |> wrapDelegate<Action<'T, StreamingContext>>
        let onDeserialized = allMethods |> getSerializationMethods<OnDeserializedAttribute> |> wrapDelegate<Action<'T, StreamingContext>>

        let isDeserializationCallback = isAssignableFrom typeof<IDeserializationCallback> typeof<'T>
        let isMarshaledObjRef = 
#if NETSTANDARD2_0
            false
#else
             //20221115 改回去?
//            false //2021 anibal for publish.bat
            typeof<'T> = typeof<System.Runtime.Remoting.ObjectHandle>
#endif
        let isObjectReference = isAssignableFrom typeof<IObjectReference> typeof<'T>

        let inline run (dele : Action<'T, StreamingContext> []) w x =
            for d in dele do d.Invoke(x, getStreamingContext w)

#if EMIT_IL
        let ctorDele = wrapISerializableConstructor<'T> ctorInfo

        let inline create si sc = ctorDele.Invoke(si, sc)
#else
        let inline create (si : SerializationInfo) (sc : StreamingContext) = 
            ctorInfo.Invoke [| si :> obj ; sc :> obj |] |> fastUnbox<'T>
#endif
        let writer (w : WriteState) (_ : string) (t : 'T) =
            checkSerializationSubtype w t
            run onSerializing w t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, w.StreamingContext)
            writeSerializationInfo w sI
            run onSerialized w t

        let reader (r : ReadState) (_ : string) =
            checkDeserializationSubtype<'T> r
            let sI = readSerializationInfo<'T> r
            let t = create sI r.StreamingContext
            run onDeserialized r t
            if isDeserializationCallback then (fastUnbox<IDeserializationCallback> t).OnDeserialization null
            if isObjectReference && not isMarshaledObjRef then 
                (fastUnbox<IObjectReference> t).GetRealObject r.StreamingContext :?> 'T
            else
                t

        let cloner (c : CloneState) (t : 'T) =
            run onSerializing c t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, c.StreamingContext)
            run onSerialized c t

            let sI' = cloneSerializationInfo<'T> c sI
            let t' = create sI' c.StreamingContext
            run onDeserialized c t'
            if isDeserializationCallback then (fastUnbox<IDeserializationCallback> t').OnDeserialization null
            if isObjectReference && not isMarshaledObjRef then 
                (fastUnbox<IObjectReference> t').GetRealObject c.StreamingContext :?> 'T
            else
                t'

        let accepter (v : VisitState) (t : 'T) =
            run onSerializing v t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, v.StreamingContext)
            run onSerialized v t
            acceptSerializationInfo v sI

        CompositePickler.Create(reader, writer, cloner, accepter, PicklerInfo.ISerializable)

    static member CreateObjectReferencePickler<'T when 'T :> ISerializable> () : Pickler<'T> =
        let allMethods = typeof<'T>.GetMethods(allMembers)
        let onSerializing = allMethods |> getSerializationMethods<OnSerializingAttribute> |> wrapDelegate<Action<'T, StreamingContext>>
        let onSerialized = allMethods |> getSerializationMethods<OnSerializedAttribute> |> wrapDelegate<Action<'T, StreamingContext>>
        let onDeserialized = allMethods |> getSerializationMethods<OnDeserializedAttribute> |> wrapDelegate<Action<'T, StreamingContext>>

        let isDeserializationCallback = isAssignableFrom typeof<IDeserializationCallback> typeof<'T>

        let inline run (dele : Action<'T, StreamingContext> []) w x =
            for d in dele do d.Invoke(x, getStreamingContext w)

        let writer (w : WriteState) (_ : string) (t : 'T) =
            checkSerializationSubtype w t
            run onSerializing w t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, w.StreamingContext)
            if not <| isAssignableFrom typeof<IObjectReference> sI.ObjectType then
                raise <| new NonSerializableTypeException(typeof<'T>, "is ISerializable but does not implement deserialization constructor or use IObjectReference.")
            w.TypePickler.Write w "ObjectType" sI.ObjectType
            writeSerializationInfo w sI
            run onSerialized w t

        let reader (r : ReadState) (_ : string) =
            checkDeserializationSubtype<'T> r
            let objectType = r.TypePickler.Read r "ObjectType"
            let sI = readSerializationInfo<'T> r
            let objectRef =
                match objectType.TryGetConstructor [| typeof<SerializationInfo> ; typeof<StreamingContext> |] with
                | None -> FormatterServices.GetUninitializedObject(objectType) :?> IObjectReference
                | Some ctor -> 
                    sI.SetType objectType
                    ctor.Invoke [| sI :> obj ; r.StreamingContext :> obj |] :?> IObjectReference

            let t = objectRef.GetRealObject r.StreamingContext :?> 'T
            run onDeserialized r t
            if isDeserializationCallback then (fastUnbox<IDeserializationCallback> t).OnDeserialization null
            t

        let cloner (c : CloneState) (t : 'T) =
            run onSerializing c t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, c.StreamingContext)
            if not <| isAssignableFrom typeof<IObjectReference> sI.ObjectType then
                raise <| new NonSerializableTypeException(typeof<'T>, "is ISerializable but does not implement deserialization constructor or use IObjectReference.")

            run onSerialized c t

            let objectRef =
                match sI.ObjectType.TryGetConstructor [| typeof<SerializationInfo> ; typeof<StreamingContext> |] with
                | None -> FormatterServices.GetUninitializedObject(sI.ObjectType) :?> IObjectReference
                | Some ctor -> 
                    let sI' = cloneSerializationInfo<'T> c sI
                    sI'.SetType sI.ObjectType
                    ctor.Invoke [| sI' :> obj ; c.StreamingContext :> obj |] :?> IObjectReference

            let t' = objectRef.GetRealObject c.StreamingContext :?> 'T
            run onDeserialized c t'
            if isDeserializationCallback then (fastUnbox<IDeserializationCallback> t').OnDeserialization null
            t'

        let accepter (v : VisitState) (t : 'T) =
            run onSerializing v t
            let sI = mkSerializationInfo<'T> ()
            t.GetObjectData(sI, v.StreamingContext)
            run onSerialized v t
            acceptSerializationInfo v sI

        CompositePickler.Create(reader, writer, cloner, accepter, PicklerInfo.ISerializable)

    static member CreateNonISerializableExceptionPickler<'Exn when 'Exn :> exn and 'Exn : not struct> registry (resolver : IPicklerResolver) =
        let exnFieldPickler = ClassFieldPickler.Create<'Exn> registry resolver :?> CompositePickler<'Exn>

        let writer (w : WriteState) (tag : string) (e : 'Exn) =
            checkSerializationSubtype w e
            let sI = mkSerializationInfo<'Exn> ()
            exnFieldPickler.Writer w tag e
            e.GetObjectData(sI, w.StreamingContext)
            writeSerializationInfo w sI

        let reader (r : ReadState) (tag : string) =
            checkDeserializationSubtype<'Exn> r
            let e = exnFieldPickler.Reader r tag
            let sI = readSerializationInfo<'Exn> r
            let dummyExn = mkDummyException sI r.StreamingContext
            ShallowObjectCopier<Exception>.Copy dummyExn e
            e

        let cloner (c : CloneState) (e : 'Exn) =
            let sI = mkSerializationInfo<'Exn> ()
            let e' = exnFieldPickler.Cloner c e
            e.GetObjectData(sI, c.StreamingContext)
            let sI' = cloneSerializationInfo<'Exn> c sI
            let dummyExn = mkDummyException sI' c.StreamingContext
            ShallowObjectCopier<Exception>.Copy dummyExn e'
            e'

        let accepter (v : VisitState) (e : 'Exn) =
            let sI = mkSerializationInfo<'Exn> ()
            exnFieldPickler.Accept v e
            e.GetObjectData(sI, v.StreamingContext)
            acceptSerializationInfo v sI

        CompositePickler.Create(reader, writer, cloner, accepter, PicklerInfo.ISerializable)

    /// SerializationInfo-based pickler combinator
    static member FromSerializationInfo<'T>(ctor : SerializationInfo -> 'T, proj : SerializationInfo -> 'T -> unit, ?useWithSubtypes) : Pickler<'T> =
        let writer (state : WriteState) (_ : string) (t : 'T) =
            checkSerializationSubtype state t
            let sI = mkSerializationInfo<'T> ()
            do proj sI t
            writeSerializationInfo state sI

        let reader (state : ReadState) (_ : string) =
            checkDeserializationSubtype<'T> state
            let sI = readSerializationInfo<'T> state
            ctor sI

        let cloner (state : CloneState) (t: 'T) =
            let sI = mkSerializationInfo<'T> ()
            do proj sI t
            let sI' = cloneSerializationInfo<'T> state sI
            ctor sI'

        let accepter (v : VisitState) (t : 'T) =
            let sI = mkSerializationInfo<'T> ()
            do proj sI t
            acceptSerializationInfo v sI

        CompositePickler.Create(reader, writer, cloner, accepter, PicklerInfo.Combinator, ?useWithSubtypes = useWithSubtypes)
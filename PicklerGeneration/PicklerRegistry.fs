namespace MBrace.FsPickler

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Reflection
open Microsoft.FSharp.Core
open Microsoft.FSharp.Quotations

type PicklerFactory =
| NonGeneric of (IPicklerResolver -> Pickler)
| Generic of (Type -> Type[] -> IPicklerResolver -> Pickler)

//type MakePicklerFactory<'T> =
//| MNonGeneric of (IPicklerResolver -> Pickler)
//| MGeneric of (IPicklerResolver -> Pickler<'T>)
//| MGenericWithArg of (IPicklerResolver -> Pickler<'T>)

/// Abstraction for specifying user-supplied custom picklers
type ICustomPicklerRegistry =
    /// Look up pickler registration for particular type
    abstract GetRegistration : Type -> CustomPicklerRegistration

/// Pickler registration for particular type
and CustomPicklerRegistration =
    /// Pickler not registered for particular type
    | UnRegistered
    /// Declares a type serializable, but uses default pickler generators for it
    | DeclaredSerializable
    /// Declares a custom pickler factor for the given type
    | CustomPickler of factory:(IPicklerResolver -> Pickler)
    //| CustomPicklerT of factory:(Type[] -> IPicklerResolver -> Pickler)

[<AutoOpen>]
module internal PicklerRegistryExtensions =

    type ICustomPicklerRegistry with
        member __.IsDeclaredSerializable(t : Type) = 
            match __.GetRegistration t with
            | DeclaredSerializable -> true
            | _ -> false

        member __.TryGetPicklerFactory(t : Type) =
            match __.GetRegistration t with
            | CustomPickler f -> Some f
            | _ -> None

/// A pickler registry with no items
[<Sealed; AutoSerializable(false)>]
type EmptyPicklerRegistry() =
    interface ICustomPicklerRegistry with
        member __.GetRegistration _ = UnRegistered

type RC () =
    static member val resolverCache = new ConcurrentDictionary<Type[]*Type, MethodInfo>() with get, set

type MC () =
    static member val fCache = new ConcurrentDictionary<Type,(Type [] -> IPicklerResolver -> Pickler)>() with get, set

//type PC<'T> (factory : Func<IPicklerResolver, Pickler<'T>>) =
//    member this.f r = factory.Invoke r


type PCBase<'S> () =
    static member val fInstance = Unchecked.defaultof<IPicklerResolver -> Pickler<'S>> with get, set
    static member ff<'T> () = PCBase<'T>.fInstance

type PC () =
    static member f<'T> (factory : FSharpFunc<IPicklerResolver, Pickler<'T>>, r) = 
        factory r

    static member f2<'T> (factory : Func<IPicklerResolver, Pickler<'T>>, r:IPicklerResolver) = 
        factory.Invoke r


type PC2<'T> (factory : IPicklerResolver -> Pickler<'T>) =
    member this.f (r) = 
        factory r

    static member f2<'T> (factory : Func<IPicklerResolver, Pickler<'T>>, r:IPicklerResolver) = 
        factory.Invoke r



/// Type for appending user-supplied pickler registrations
[<Sealed; AutoSerializable(false)>]
type CustomPicklerRegistry () =
    let syncRoot = obj()
    let mutable isGenerationStarted = false

    let picklerFactories = new Dictionary<Type, PicklerFactory>()
    let typesDeclaredSerializable = new HashSet<Type>()
    let serializationPredicates = new ResizeArray<Type -> bool>()

    let rec checkNoObjType (t: Type) : bool =
        // 如果 t 是泛型類型，則檢查其泛型參數
        if t.IsGenericType then
            let args = t.GetGenericArguments()
            printfn "t: %s" t.Name
            // 檢查每個泛型參數，並遞歸處理嵌套類型
            args |> Array.forall (fun arg -> 
                printfn "arg: %s" arg.Name
                if arg = typeof<obj> then 
                    false // 如果發現 typeof<obj>，則返回 false
                else
                    checkNoObjType arg // 递归检查嵌套类型
            )
        else
            true 

    // 遞歸處理泛型參數，將 typeof<obj> 替換為具體類型
    let rec processGenericArguments (args: Type[]) (concreteType: Type) =
        args |> Array.map (fun arg ->
            if arg = typeof<obj> then
                concreteType // 將 obj 替換為具體的類型參數
            elif arg.IsGenericType then
                // 如果遇到嵌套的泛型類型，遞歸處理
                let innerArgs = arg.GetGenericArguments()
                let genericDef = arg.GetGenericTypeDefinition()
                genericDef.MakeGenericType(processGenericArguments innerArgs concreteType)
            else
                arg
        )

    let registerPickler f =
        lock syncRoot (fun () ->
            if not isGenerationStarted then f ()
            else
                invalidOp "this pickler registry is already in use by a serializer, cannot register new instances.")

    let getPicklerRegistration (t : Type) =
        if not isGenerationStarted then lock syncRoot (fun () -> isGenerationStarted <- true)

        //let ok, f = 
        //    if picklerFactories.ContainsKey t then
        //        picklerFactories.TryGetValue t
        //    elif t.IsGenericType then
        //        let gtd = t.GetGenericTypeDefinition()
        //        picklerFactories.TryGetValue gtd
        //    else
        //        false, Unchecked.defaultof<_>

                
        //if ok then CustomPickler f 
        let (ok, f), ifGeneric = 
            if picklerFactories.ContainsKey t then
                picklerFactories.TryGetValue t, 1
            elif t.IsGenericType then
                let gtd = t.GetGenericTypeDefinition()
                picklerFactories.TryGetValue gtd, 2
            else
                (false, Unchecked.defaultof<_>), 0

                
        if ok then 
            if ifGeneric = 1 then
                let (NonGeneric ngf) = f
                CustomPickler ngf
            else
                let (Generic gf) = f
                //CustomPicklerT gf
                let ngf = gf t (t.GetGenericArguments())
                CustomPickler ngf
        elif typesDeclaredSerializable.Contains t then DeclaredSerializable
        elif serializationPredicates |> Seq.exists (fun p -> p t) then DeclaredSerializable
        else UnRegistered

    /// Gets whether isntance is alread being used for pickler generation.
    /// In that case, any attempt to register new types will result in an InvalidOperationException
    member __.IsGenerationStarted = isGenerationStarted
    /// List of all individual types declared serializable
    member __.TypesDeclaredSerializable = typesDeclaredSerializable |> Seq.toArray
    /// List of all individual pickler factory types
    member __.PicklerFactories = picklerFactories.Keys |> Seq.toArray
    /// List of all user-specified custom serialization predicates
    member __.SerializationPredicates = serializationPredicates.ToArray()

    /// Registers a supplied custom pickler
    member __.RegisterPickler(pickler : Pickler) : unit =
        registerPickler (fun () -> picklerFactories.[pickler.Type] <- NonGeneric (fun _ -> pickler))

    /// Registers a collections of supplied custom pickelrs
    member __.RegisterPicklers([<ParamArray>] picklers : Pickler[]) : unit =
        for p in picklers do __.RegisterPickler p

    /// Registers a user-specified pickler factory
    
    //member __.RegisterFactory<'T>(factory : IPicklerResolver -> Pickler) : unit =
    //    let tObj = typeof<'T>
    //    if checkNoObjType tObj then
    //        let td = typedefof<'T>
    //        registerPickler (fun () -> picklerFactories.[td] <- NonGeneric factory)        
        
    //    elif tObj.IsGenericType || tObj.IsGenericTypeDefinition then     
    //        let td = typedefof<'T>
    //        registerPickler (fun () -> picklerFactories.[td] <- NonGeneric factory)
    //    else
    //        registerPickler (fun () -> picklerFactories.[typeof<'T>] <- NonGeneric factory)

    member __.RegisterFactory<'T>(factory : Type -> Type[] -> IPicklerResolver -> Pickler) : unit =
        let td = typedefof<'T>
        registerPickler (fun () -> picklerFactories.[td] <- Generic (fun t tArr r -> factory t tArr r)) 

    member __.RegisterFactory<'T>(factory : IPicklerResolver -> Pickler<'T>) : unit =
        let tObj = typeof<'T>
        if checkNoObjType tObj then
            let td = typedefof<'T>
            registerPickler (fun () -> picklerFactories.[td] <- NonGeneric (fun r -> factory r :> Pickler))        
        
        //elif tObj.IsGenericType || tObj.IsGenericTypeDefinition then            
        //    let td = typedefof<'T>            
        //    let tf = typedefof<Func<IPicklerResolver, Pickler<'T>>>
        //    let modF = Func<IPicklerResolver, Pickler<'T>> factory
        //    let gm = modF.GetMethodInfo().GetGenericMethodDefinition()
        //    let ff : Converter<_, Pickler<'T>> = factory

            
            

        //    let pci tArr (r:IPicklerResolver) = 
        //        let typArg = td.MakeGenericType(tArr)
        //        let pcm = (typeof<PC>.GetMethod "f").MakeGenericMethod([|typArg|])
        //        //let f : IPicklerResolver -> Pickler<'T> = 
        //        //    fun iresolver -> 
        //        //        let p = factory iresolver 
        //        //        p :> Pickler |> box :?> Pickler<'T>
                    
        //        //pcm.Invoke(PC(), [| box (Func<IPicklerResolver, Pickler<'T>> f); box r |])
        //        pcm.Invoke(PC(), [| box factory; box r |])

        //    //let f : Expr<IPicklerResolver -> Pickler<'T>> = <@ factory @>

        //    let invoker (tArr:Type[]) (r:IPicklerResolver) =
        //    //    //let processedArr = processGenericArguments tArr typeof<'S>
        //        let typArg = td.MakeGenericType(tArr)
        //        let containerTyp = typedefof<Pickler<'T>>.MakeGenericType([|typArg|])
        //        //let resolverType = r.GetType()
        //        let fConcrete = tf.MakeGenericType([|typeof<IPicklerResolver>; containerTyp|]) 
        //    //    let f = 
        //        let mi = fConcrete.GetMethod "Invoke"
        //        mi.Invoke(modF, [|r|]) :?> Pickler

        //    let u = Func<_, _, _> (fun _ _ -> invoker)
              
        //    MC.fCache.AddOrUpdate (
        //        td
        //        , invoker
        //        , u
        //    ) |> ignore

        //    registerPickler (fun () -> 
        //        picklerFactories.[td] <- Generic (fun t tArr resolver -> 
        //            let rt = resolver.GetType()
        //            let ta = typeof<'T>
                    
        //            let pInstance = pci tArr resolver
                    
        //            pInstance :?> Pickler
        //            //let miFun = MC.fCache[t.GetGenericTypeDefinition()]

        //            //miFun tArr resolver

        //            //let mi = 
        //            //    RC.resolverCache.GetOrAdd (
        //            //        (tArr, rt)
        //            //        , (fun _ ->
        //            //            invoker tArr rt                                
        //            //        )
        //            //    )
        //            //let ivkRst = mi.Invoke(modF, [|resolver|]) 
        //            //ivkRst :?> Pickler
        //            )
        //        )
        
        else
            registerPickler (fun () -> picklerFactories.[typeof<'T>] <- NonGeneric (fun r -> factory r :> Pickler))


(*
  
    member __.RegisterFactoryT<'T>(makeFactory : MakePicklerFactory<'T>) : unit =
        match makeFactory with
        | MNonGeneric factory ->
            registerPickler (fun () -> picklerFactories.[typeof<'T>] <- NonGeneric factory)
        | MGeneric factory ->
            if typeof<'T>.IsGenericTypeDefinition then
                failwithf "GenericTypeDefinition requires full factory function form"
            elif typeof<'T>.IsGenericType then
                let td = typedefof<'T>
                registerPickler (fun () -> picklerFactories.[td] <- NonGeneric (fun r -> factory r :> Pickler))
            else 
                registerPickler (fun () -> picklerFactories.[typeof<'T>] <- NonGeneric (fun r -> factory r :> Pickler))

        | MGenericWithArg factory ->
            if typeof<'T>.IsGenericTypeDefinition then
                let td = typedefof<'T>
                
                let tf = typedefof<#IPicklerResolver -> 'T>

                let invoker (tArr:Type[]) (resolverType:Type) =
                    let containerTyp = td.MakeGenericType(tArr)
                    tf.MakeGenericType([|resolverType; containerTyp|]).GetMethod "Invoke"

                registerPickler (fun () -> 
                    picklerFactories.[td] <- Generic (fun tArr resolver -> 
                        let rt = resolver.GetType()
                        
                        let mi = 
                            RC.resolverCache.GetOrAdd (
                                (tArr, rt)
                                , (fun _ ->
                                    invoker tArr rt                                
                                )
                            )
                        mi.Invoke(factory, [|resolver|]) :?> Pickler
                        )
                    )
        
            else 
                failwithf "Non generic arg factory provided!"
*)
    /// Appends a predicate used to determine whether a specific type should be treated as if carrying the .IsSerializable flag
    member __.DeclareSerializable (isSerializable : Type -> bool) : unit =
        registerPickler (fun () -> serializationPredicates.Add isSerializable)

    /// Appends a list of types that will be treated by the pickler generator as if carrying the .IsSerializable flag
    member __.DeclareSerializable ([<ParamArray>] typesToSerialize : Type[]) : unit =
        registerPickler (fun () -> for t in typesToSerialize do typesDeclaredSerializable.Add t |> ignore)

    /// Registers the specifed type as if carrying the .IsSerializable flag
    member __.DeclareSerializable<'T> () : unit =
        __.DeclareSerializable(typeof<'T>)

    interface ICustomPicklerRegistry with
        member __.GetRegistration t = getPicklerRegistration t
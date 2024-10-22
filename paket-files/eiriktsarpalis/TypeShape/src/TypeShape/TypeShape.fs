#if TYPESHAPE_EXPOSE
[<AutoOpen>]
module TypeShape.Core.Core
#else
// NB we don't want to leak the `TypeShape` namespace
// to the public API of the assembly
// so we use a top-level internal module
module internal TypeShape
#endif

#nowarn "4224"

open System
open System.Collections.Generic
open System.ComponentModel
open System.Runtime.CompilerServices
open System.Runtime.Serialization
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Reflection

//------------------------------------
// Section: TypeShape core definitions

/// Provides a simple breakdown of basic kinds of types.
/// Used for easier extraction of type shapes in the active pattern implementations.
[<NoEquality; NoComparison>]
type TypeShapeInfo =
    | Basic of Type
    | Enum of enumTy:Type * underlying:Type
    | Array of element:Type * rank:int
    | Generic of definition:Type * args:Type []

/// Used to extract the type variable contained in a specific shape
type ITypeVisitor<'R> =
    abstract Visit<'T> : unit -> 'R

/// Encapsulates a type variable that can be accessed using type shape visitors
[<AbstractClass>]
type TypeShape =
    [<CompilerMessage("TypeShape constructor should not be consumed.", 4224); EditorBrowsable(EditorBrowsableState.Never)>]
    internal new () = { }
    abstract Type : Type
    abstract ShapeInfo : TypeShapeInfo
    abstract Accept : ITypeVisitor<'R> -> 'R

/// Encapsulates a type variable that can be accessed using type shape visitors
[<Sealed>]
type TypeShape<'T> private () =
    inherit TypeShape()

    static let shapeInfo =
        let t = typeof<'T>
        if t.IsEnum then
            Enum(t, Enum.GetUnderlyingType t)
        elif t.IsArray then
            Array(t.GetElementType(), t.GetArrayRank())
        elif t.IsGenericType then 
            Generic(t.GetGenericTypeDefinition(), t.GetGenericArguments())
        else
            Basic t

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member val internal Instance = new TypeShape<'T>()
        
    override _.Type = typeof<'T>
    override _.ShapeInfo = shapeInfo
    override _.Accept v = v.Visit<'T> ()
    override _.Equals o = o :? TypeShape<'T>
    override _.GetHashCode() = hash typeof<'T>

exception UnsupportedShape of Type:Type
    with
    override e.Message = sprintf "Unsupported TypeShape '%O'" e.Type

[<AutoOpen>]
module private TypeShapeImpl =

    let fsharpCoreRuntimeVersion = typeof<unit>.Assembly.GetName().Version
    let fsharpCore41Version = Version(4,4,1,0)

    [<Literal>]
    let AllMembers =
        BindingFlags.NonPublic ||| BindingFlags.Public |||
            BindingFlags.Instance ||| BindingFlags.Static |||
                BindingFlags.FlattenHierarchy 

    [<Literal>]
    let AllInstanceMembers =
        BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance

    type MemberInfo with
        member inline m.ContainsAttr<'Attr when 'Attr :> Attribute> (inheritAttr) =
            m.GetCustomAttributes(inheritAttr)
            |> Array.exists (function :? 'Attr -> true | _ -> false)

        member inline m.TryGetAttribute<'Attr when 'Attr :> Attribute> (inheritAttr) =
            m.GetCustomAttributes(inheritAttr)
            |> Array.tryPick (function :? 'Attr as attr -> Some attr | _ -> None)

    let activateGeneric (templateTy:Type) (typeArgs : Type[]) (args:obj[]) =
        let templateTy =
            if typeArgs.Length = 0 then templateTy
            elif not templateTy.IsGenericType then invalidArg (string templateTy) "not generic."
            elif not templateTy.IsGenericTypeDefinition then
                templateTy.GetGenericTypeDefinition().MakeGenericType typeArgs
            else
                templateTy.MakeGenericType typeArgs

        let ctypes = args |> Array.map (fun o -> o.GetType())
        let ctor = templateTy.GetConstructor(AllMembers, null, CallingConventions.Standard, ctypes, [||])
        ctor.Invoke args

    /// correctly resolves if type is assignable to interface
    let rec isInterfaceAssignableFrom (iface : Type) (ty : Type) =
        let proj (t : Type) = t.Assembly, t.Namespace, t.Name, t.MetadataToken
        if iface = ty then true
        elif ty.GetInterfaces() |> Array.exists(fun if0 -> proj if0 = proj iface) then true
        else
            match ty.BaseType with
            | null -> false
            | bt -> isInterfaceAssignableFrom iface bt

    // walks the type hierarchy for fields
    let gatherMembers (f : Type -> #seq<'Member>) (t : Type) =
        let members = ResizeArray()
        let rec aux (t : Type) =
            match t.BaseType with
            | null -> ()
            | bt ->
                aux bt
                members.AddRange(f t)

        aux t
        members.ToArray()

    type private ReflectionHelper =
        static member GetInstance<'T>() = TypeShape<'T>.Instance
    
    let private genInstanceGetter = typeof<ReflectionHelper>.GetMethod("GetInstance", BindingFlags.NonPublic ||| BindingFlags.Static)
    let private canon = Type.GetType "System.__Canon"

    let inline internal isUnsupported (typ: Type) =
        typ.IsPointer ||
        typ.IsByRef
#if NETCOREAPP
        || typ.IsByRefLike
#endif

    let resolveTypeShape(typ : Type) =
        if isNull typ then raise <| ArgumentNullException("TypeShape: System.Type cannot be null.")

        if  typ.IsGenericTypeDefinition ||
            typ.IsGenericParameter ||
            typ = canon ||
            isUnsupported typ
        then 
            raise <| UnsupportedShape typ

        let genInstanceGetter = genInstanceGetter.MakeGenericMethod [|typ|]
        genInstanceGetter.Invoke(null, [||]) :?> TypeShape

type Activator with
    /// Generic edition of the activator method which support type parameters and private types
    static member CreateInstanceGeneric<'Template>(?typeArgs : Type[], ?args:obj[]) : obj =
        let typeArgs = defaultArg typeArgs [||]
        let args = defaultArg args [||]
        activateGeneric typeof<'Template> typeArgs args

type Type with
    /// Correctly resolves if type is assignable to interface
    member iface.IsInterfaceAssignableFrom(ty : Type) : bool =
        isInterfaceAssignableFrom iface ty

type TypeShape with
    /// <summary>
    ///     Creates a type shape instance for given type
    /// </summary>
    /// <param name="typ">System.Type to be resolved.</param>
    static member Create(typ : Type) : TypeShape = resolveTypeShape typ

    /// <summary>
    ///     Creates a type shape instance from the underlying
    ///     type of a given value.
    /// </summary>
    /// <param name="obj">Non-null value to extract shape data from.</param>
    static member FromValue(obj : obj) : TypeShape =
        match obj with
        | null -> raise <| ArgumentNullException()
        | obj -> resolveTypeShape (obj.GetType())

    /// <summary>
    ///     Creates a type shape instance for given type
    /// </summary>
    static member Create<'T>() : TypeShape<'T> = TypeShape<'T>.Instance

/// Creates a type shape instance for given type
let shapeof<'T> = TypeShape<'T>.Instance

//------------------------
// Section: Core BCL types

// Enum types

type IEnumVisitor<'R> =
    abstract Visit<'Enum, 'Underlying when 'Enum : enum<'Underlying>
                                       and 'Enum : struct
                                       and 'Enum :> ValueType
                                       and 'Enum : (new : unit -> 'Enum)> : unit -> 'R

type IShapeEnum =
    abstract Underlying : TypeShape
    abstract Accept : IEnumVisitor<'R> -> 'R

type private ShapeEnum<'Enum, 'Underlying when 'Enum : enum<'Underlying>
                                           and 'Enum : struct
                                           and 'Enum :> ValueType
                                           and 'Enum : (new : unit -> 'Enum)>() =
    interface IShapeEnum with
        member _.Underlying = shapeof<'Underlying> :> _
        member _.Accept v = v.Visit<'Enum, 'Underlying> ()

// Nullable types

type INullableVisitor<'R> =
    abstract Visit<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> : unit -> 'R

type IShapeNullable =
    abstract Element : TypeShape
    abstract Accept : INullableVisitor<'R> -> 'R

type private ShapeNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () =
    interface IShapeNullable with
        member _.Element = shapeof<'T> :> _
        member _.Accept v = v.Visit<'T> ()


// Default Constructor types

type IDefaultConstructorVisitor<'R> =
    abstract Visit<'T when 'T : (new : unit -> 'T)> : unit -> 'R

type IShapeDefaultConstructor =
    abstract Accept : IDefaultConstructorVisitor<'R> -> 'R

type private ShapeDefaultConstructor<'T when 'T : (new : unit -> 'T)>() =
    interface IShapeDefaultConstructor with
        member _.Accept v = v.Visit<'T>()

// Equality Types
    
type IEqualityVisitor<'R> =
    abstract Visit<'T when 'T : equality> : unit -> 'R

type IShapeEquality =
    abstract Accept : IEqualityVisitor<'R> -> 'R

type private ShapeEquality<'T when 'T : equality>() =
    interface IShapeEquality with
        member _.Accept v = v.Visit<'T>()

// Comparison Types
    
type IComparisonVisitor<'R> =
    abstract Visit<'T when 'T : comparison> : unit -> 'R

type IShapeComparison =
    abstract Accept : IComparisonVisitor<'R> -> 'R

type private ShapeComparison<'T when 'T : comparison>() =
    interface IShapeComparison with
        member _.Accept v = v.Visit<'T>()

// Struct Types

type IStructVisitor<'R> =
    abstract Visit<'T when 'T : struct> : unit -> 'R

type IShapeStruct =
    abstract Accept : IStructVisitor<'R> -> 'R

type private ShapeStruct<'T when 'T : struct>() =
    interface IShapeStruct with
        member _.Accept v = v.Visit<'T>()

// Reference Types

type INotStructVisitor<'R> =
    abstract Visit<'T when 'T : not struct and 'T : null> : unit -> 'R

type IShapeNotStruct =
    abstract Accept : INotStructVisitor<'R> -> 'R

type private ShapeNotStruct<'T when 'T : not struct and 'T : null>() =
    interface IShapeNotStruct with
        member _.Accept v = v.Visit<'T>()

// Delegates

type IDelegateVisitor<'R> =
    abstract Visit<'Delegate when 'Delegate :> Delegate> : unit -> 'R

type IShapeDelegate =
    abstract Accept : IDelegateVisitor<'R> -> 'R

type private ShapeDelegate<'Delegate when 'Delegate :> Delegate>() =
    interface IShapeDelegate with
        member _.Accept v = v.Visit<'Delegate>()

// F# functions

type IFSharpFuncVisitor<'R> =
    abstract Visit<'Domain, 'CoDomain> : unit -> 'R

type IShapeFSharpFunc =
    abstract Domain : TypeShape
    abstract CoDomain : TypeShape
    abstract Accept : IFSharpFuncVisitor<'R> -> 'R

type private ShapeFSharpFunc<'Domain, 'CoDomain> () =
    interface IShapeFSharpFunc with
        member _.Domain = shapeof<'Domain> :> _
        member _.CoDomain = shapeof<'CoDomain> :> _
        member _.Accept v = v.Visit<'Domain, 'CoDomain> ()

// System.Exception

type IExceptionVisitor<'R> =
    abstract Visit<'exn when 'exn :> exn and 'exn : not struct and 'exn : null> : unit -> 'R

type IShapeException =
    abstract IsFSharpException : bool
    abstract Accept : IExceptionVisitor<'R> -> 'R

type private ShapeException<'exn when 'exn :> exn and 'exn : not struct and 'exn : null> (isFSharpExn : bool) =
    interface IShapeException with
        member _.IsFSharpException = isFSharpExn
        member _.Accept v = v.Visit<'exn> ()


//-----------------------------------
// Section: Collections & IEnumerable

// IEnumerable

type IEnumerableVisitor<'R> =
    abstract Visit<'Enum, 'T when 'Enum :> seq<'T>> : unit -> 'R

type IShapeEnumerable =
    abstract Element : TypeShape
    abstract Accept : IEnumerableVisitor<'R> -> 'R

type private ShapeEnumerable<'Enum, 'T when 'Enum :> seq<'T>> () =
    interface IShapeEnumerable with
        member _.Element = shapeof<'T> :> _
        member _.Accept v = v.Visit<'Enum, 'T> ()

// Collection

type ICollectionVisitor<'R> =
    abstract Visit<'Collection, 'T when 'Collection :> ICollection<'T>> : unit -> 'R

type IShapeCollection =
    abstract Element : TypeShape
    abstract Accept : ICollectionVisitor<'R> -> 'R

type private ShapeCollection<'Collection, 'T when 'Collection :> ICollection<'T>> () =
    interface IShapeCollection with
        member _.Element = shapeof<'T> :> _
        member _.Accept v = v.Visit<'Collection, 'T> ()

// KeyValuePair

type IKeyValuePairVisitor<'R> =
    abstract Visit<'K, 'V> : unit -> 'R

type IShapeKeyValuePair =
    abstract Key : TypeShape
    abstract Value : TypeShape
    abstract Accept : IKeyValuePairVisitor<'R> -> 'R

type private ShapeKeyValuePair<'K,'V> () =
    interface IShapeKeyValuePair with
        member _.Key = shapeof<'K> :> _
        member _.Value = shapeof<'V> :> _
        member _.Accept v = v.Visit<'K, 'V> ()

// System.Array

type IShapeArray =
    /// Gets the rank of the array type shape
    abstract Rank : int
    abstract Element : TypeShape

type private ShapeArray<'T>(rank : int) =
    interface IShapeArray with
        member _.Rank = rank
        member _.Element = shapeof<'T> :> _

type ISystemArrayVisitor<'R> =
    abstract Visit<'Array when 'Array :> System.Array> : unit -> 'R

type IShapeSystemArray =
    abstract Rank : int
    abstract Element : TypeShape
    abstract Accept : ISystemArrayVisitor<'R> -> 'R

type private ShapeSystemArray<'Array when 'Array :> System.Array>(elem : Type, rank : int) =
    interface IShapeSystemArray with
        member _.Rank = rank
        member _.Element = TypeShape.Create elem
        member _.Accept v = v.Visit<'Array> ()
    

// System.Collections.List

type IResizeArrayVisitor<'R> =
    abstract Visit<'T> : unit -> 'R

type IShapeResizeArray =
    abstract Element : TypeShape

type private ShapeResizeArray<'T> () =
    interface IShapeResizeArray with
        member _.Element = shapeof<'T> :> _


// System.Collections.Dictionary

type IDictionaryVisitor<'R> =
    abstract Visit<'K, 'V when 'K : equality> : unit -> 'R

type IShapeDictionary =
    abstract Key : TypeShape
    abstract Value : TypeShape
    abstract Accept : IDictionaryVisitor<'R> -> 'R

type private ShapeDictionary<'K, 'V when 'K : equality> () =
    interface IShapeDictionary with
        member _.Key = shapeof<'K> :> _
        member _.Value = shapeof<'V> :> _
        member _.Accept v = v.Visit<'K, 'V> ()

// System.Collections.HashSet

type IHashSetVisitor<'R> =
    abstract Visit<'T when 'T : equality> : unit -> 'R

type IShapeHashSet =
    abstract Element : TypeShape
    abstract Accept : IHashSetVisitor<'R> -> 'R

type private ShapeHashSet<'T when 'T : equality> () =
    interface IShapeHashSet with
        member _.Element = shapeof<'T> :> _
        member _.Accept v = v.Visit<'T> ()

// F# Set

type IFSharpSetVisitor<'R> =
    abstract Visit<'T when 'T : comparison> : unit -> 'R

type IShapeFSharpSet =
    abstract Element : TypeShape
    abstract Accept : IFSharpSetVisitor<'R> -> 'R

type private ShapeFSharpSet<'T when 'T : comparison> () =
    interface IShapeFSharpSet with
        member _.Element = shapeof<'T> :> _
        member _.Accept v = v.Visit<'T> ()

// F# Map

type IFSharpMapVisitor<'R> =
    abstract Visit<'K, 'V when 'K : comparison> : unit -> 'R

type IShapeFSharpMap =
    abstract Key : TypeShape
    abstract Value : TypeShape
    abstract Accept : IFSharpMapVisitor<'R> -> 'R

type private ShapeFSharpMap<'K, 'V when 'K : comparison> () =
    interface IShapeFSharpMap with
        member _.Key = shapeof<'K> :> _
        member _.Value = shapeof<'V> :> _
        member _.Accept v = v.Visit<'K, 'V>()

// F# ref

type IShapeFSharpRef =
    abstract Element : TypeShape

type private ShapeFSharpRef<'T> () =
    interface IShapeFSharpRef with
        member _.Element = shapeof<'T> :> _

// F# option

type IShapeFSharpOption =
    abstract Element : TypeShape

type private ShapeFSharpOption<'T> () =
    interface IShapeFSharpOption with
        member _.Element = shapeof<'T> :> _

// F# List

type IShapeFSharpList =
    abstract Element : TypeShape

type private ShapeFSharpList<'T> () =
    interface IShapeFSharpList with
        member _.Element = shapeof<'T> :> _

//-----------------------------
// Section: Member-based Shapes

[<AutoOpen>]
module private MemberUtils =

    let private untypedVisitor =
        {
            new ITypeVisitor<obj> with
                member _.Visit<'T>() = Unchecked.defaultof<'T> :> obj
        }

    let defaultOfUntyped (ty : Type) =
        TypeShape.Create(ty).Accept untypedVisitor

    let invalidMember (memberInfo : MemberInfo) =
        sprintf "TypeShape internal error: invalid MemberInfo '%O'" memberInfo
        |> invalidOp

    let isStructMember (path : MemberInfo[]) =
        path |> Array.exists (fun m -> m.DeclaringType.IsValueType)

    let isPublicMember (memberInfo : MemberInfo) =
        match memberInfo with
        | :? FieldInfo as f -> f.IsPublic
        | :? PropertyInfo as p -> p.GetGetMethod(true).IsPublic
        | _ -> invalidMember memberInfo
        
    let isWriteableMember (path : MemberInfo[]) =
        path 
        |> Array.forall (fun m ->
            match m with
            | :? FieldInfo -> true
            | :? PropertyInfo as p -> p.CanWrite
            | _ -> invalidMember m)

    let inline getValue (obj:obj) (m:MemberInfo) =
        match m with
        | :? FieldInfo as f -> f.GetValue(obj)
        | :? PropertyInfo as p -> p.GetValue(obj, null)
        | _ -> invalidMember m

    let inline setValue (obj:obj) (m:MemberInfo) (value:obj) =
        match m with
        | :? FieldInfo as f -> f.SetValue(obj, value)
        | :? PropertyInfo as p -> p.SetValue(obj, value, null)
        | _ -> invalidMember m

    let inline project<'Type, 'Member> (path : MemberInfo[]) (value:'Type) =
        let mutable obj = box value
        for m in path do
            obj <- getValue obj m
        obj :?> 'Member

    let inline inject<'Type, 'Member> (isStructMember : bool) (path : MemberInfo[]) 
                                        (instance : 'Type) (value : 'Member) =
        let n = path.Length

        // slow path; keep in a separate non-inlined function to keep the outer function small
        let handleNestedMembers () =
            assert(n > 1)
            if isStructMember then
                // we are trying to update a nested struct (e.g. a struct tuple of large arity)
                // in order to ensure that the change gets propagated to the original copy
                // we must box and update every intermediate value
                let rec update (i : int) (instance : obj) =
                    if i < n - 1 then
                        let nested = getValue instance path.[i]
                        let updated = update (i + 1) nested
                        setValue instance path.[i] updated
                        instance
                    else
                        setValue instance path.[i] value
                        instance

                update 0 instance :?> 'Type
            else
                // all nested instances are heap allocated,
                // just traverse to the leaf object and update it.
                let mutable obj = box instance
                for i = 0 to n - 2 do
                    obj <- getValue obj path.[i]

                setValue obj path.[n - 1] value
                instance

        if n = 1 then
            // Most common scenario, updating a member that is one layer deep
            if typeof<'Type>.IsValueType then
                let boxedInstance = box instance
                setValue boxedInstance path.[0] value
                boxedInstance :?> 'Type
            else
                setValue instance path.[0] value
                instance
        else
            handleNestedMembers()

#if TYPESHAPE_EXPR

    let getDefaultValueExpr (t : Type) =
        TypeShape.Create(t).Accept {
            new ITypeVisitor<Expr> with
                member _.Visit<'T> () = <@ Unchecked.defaultof<'T> @> :> _
        }

    let private castFor (m : MemberInfo) (e : Expr) =
        if m.DeclaringType = e.Type then e
        elif e.Type.IsAssignableFrom m.DeclaringType then
            Expr.Coerce(e, m.DeclaringType)
        else
            invalidOp "TypeShape: internal error, cannot cast to member declaring type."

    let projectExpr<'Record, 'Member> (path : MemberInfo[]) (expr : Expr<'Record>) =
        let rec aux expr (m:MemberInfo) =
            match m with
            | :? FieldInfo as fI -> Expr.FieldGet(castFor fI expr, fI)
            | :? PropertyInfo as pI -> Expr.PropertyGet(castFor pI expr, pI)
            | _ -> invalidMember m

        Expr.Cast<'Member>(Array.fold aux (expr :> _) path) 

    let injectExpr (path : MemberInfo[]) 
                    (r : Expr<'TRecord>) 
                    (value : Expr<'MemberType>) =
        
        if typeof<'TRecord>.IsValueType then
            // this should use Expr.AddressOf, but most quotation libs don't support it
            let rec aux i expr =
                if i = path.Length - 1 then
                    match path.[i] with
                    | :? FieldInfo as fI -> Expr.FieldSet(castFor fI expr, fI, value)
                    | :? PropertyInfo as pI -> Expr.PropertySet(castFor pI expr, pI, value)
                    | m -> invalidMember m
                else
                    let mkVar n t = Var(n, t, isMutable = true)
                    match path.[i] with
                    | :? FieldInfo as fI -> 
                        let v = mkVar fI.Name fI.FieldType
                        let getter = Expr.FieldGet(castFor fI expr, fI)
                        let nestedSetter = aux (i + 1) (Expr.Var v)
                        let setter = Expr.FieldSet(castFor fI expr, fI, Expr.Var v)
                        Expr.Let(v, getter, Expr.Sequential(nestedSetter, setter))
                    | :? PropertyInfo as pI -> 
                        let v = mkVar pI.Name pI.PropertyType
                        let getter = Expr.PropertyGet(castFor pI expr, pI)
                        let nestedSetter = aux (i + 1) (Expr.Var v)
                        let setter = Expr.PropertySet(castFor pI expr, pI, Expr.Var v)
                        Expr.Let(v, getter, Expr.Sequential(nestedSetter, setter))
                    | m -> invalidMember m

            <@ (% Expr.Cast<_>(aux 0 r)) ; %r @>
        else
            let rec aux i expr =
                if i = path.Length - 1 then
                    match path.[i] with
                    | :? FieldInfo as fI -> Expr.FieldSet(castFor fI expr, fI, value)
                    | :? PropertyInfo as pI -> Expr.PropertySet(castFor pI expr, pI, value)
                    | m -> invalidMember m
                else
                    match path.[i] with
                    | :? FieldInfo as fI -> aux (i+1) (Expr.FieldGet(castFor fI expr, fI))
                    | :? PropertyInfo as pI -> aux (i+1) (Expr.PropertyGet(castFor pI expr, pI))
                    | m -> invalidMember m
                
            <@ (% Expr.Cast<_>(aux 0 r)) ; %r @>
#endif

#if TYPESHAPE_EMIT
    open System.Reflection.Emit

    type private Marker = class end

    type Getter<'DeclaringType, 'MemberType> = delegate of inref<'DeclaringType> -> 'MemberType 
    type Setter<'DeclaringType, 'MemberType> = delegate of byref<'DeclaringType> * 'MemberType -> unit

    /// Lazily emits a dynamic method using supplied parameters
    let emitDynamicMethod<'Delegate when 'Delegate :> Delegate> (id : string) 
                (argTypes : Type []) (returnType : Type)
                (ilBuilder : ILGenerator -> unit) =

        let name = sprintf "%O-%s" typeof<'Delegate> id
        let dynamicMethod = new DynamicMethod(name, returnType, argTypes, typeof<Marker>, skipVisibility = true)
        let ilGen = dynamicMethod.GetILGenerator()
        do ilBuilder ilGen
        dynamicMethod.CreateDelegate typeof<'Delegate> :?> 'Delegate

    /// Emits a dynamic method that projects supplied member path
    let emitGetter<'DeclaringType, 'Property> (path : MemberInfo []) =
        let builder (gen : ILGenerator) =
            gen.Emit OpCodes.Ldarg_0
            if not typeof<'DeclaringType>.IsValueType then
                gen.Emit OpCodes.Ldind_Ref

            for i = 0 to path.Length - 1 do
                match path.[i] with
                | :? FieldInfo as f -> 
                    if f.FieldType.IsValueType && i < path.Length - 1 then
                        gen.Emit(OpCodes.Ldflda, f)
                    else
                        gen.Emit(OpCodes.Ldfld, f)

                | :? PropertyInfo as p -> 
                    let m = p.GetGetMethod(true)
                    if typeof<'DeclaringType>.IsValueType then
                        gen.EmitCall(OpCodes.Call, m, null)
                    else
                        gen.EmitCall(OpCodes.Callvirt, m, null)
                | _ -> invalidMember path.[i]

            gen.Emit OpCodes.Ret

        let projectionId =
            path 
            |> Seq.map (fun p -> p.Name)
            |> String.concat "->"
            |> sprintf "proj-%s"

        emitDynamicMethod<Getter<'DeclaringType, 'Property>>
            projectionId [|typeof<'DeclaringType>.MakeByRefType()|] typeof<'Property> builder

    // Emits a dynamic method that injects a value to supplied member path
    let emitSetter<'DeclaringType, 'Property> (path : MemberInfo []) =
        let builder (gen : ILGenerator) =
            gen.Emit OpCodes.Ldarg_0
            if not typeof<'DeclaringType>.IsValueType then
                gen.Emit OpCodes.Ldind_Ref

            for i = 0 to path.Length - 2 do
                match path.[i] with
                | :? FieldInfo as f -> 
                    if typeof<'DeclaringType>.IsValueType then
                        gen.Emit(OpCodes.Ldflda, f)
                    else
                        gen.Emit(OpCodes.Ldfld, f)

                | :? PropertyInfo as p ->
                    if p.DeclaringType.IsValueType then
                        invalidOp "internal error: nested struct properties not supported."

                    let m = p.GetGetMethod(true)
                    if typeof<'DeclaringType>.IsValueType then
                        gen.EmitCall(OpCodes.Call, m, null)
                    else
                        gen.EmitCall(OpCodes.Callvirt, m, null)
                | m -> invalidMember m

            gen.Emit OpCodes.Ldarg_1

            match path.[path.Length - 1] with
            | :? FieldInfo as f -> gen.Emit(OpCodes.Stfld, f)
            | :? PropertyInfo as p ->
                let m = p.GetSetMethod(true)
                if typeof<'DeclaringType>.IsValueType then
                    gen.EmitCall(OpCodes.Call, m, null)
                else
                    gen.EmitCall(OpCodes.Callvirt, m, null)

            | m -> invalidMember m

            gen.Emit OpCodes.Ret

        let injectionId =
            path 
            |> Seq.map (fun p -> p.Name)
            |> String.concat "->"
            |> sprintf "inj-%s"

        emitDynamicMethod<Setter<'DeclaringType, 'Property>>
            injectionId [|typeof<'DeclaringType>.MakeByRefType(); typeof<'Property>|] typeof<System.Void> builder

    let ldDefaultValue (gen : ILGenerator) (t : Type) =
        if not t.IsValueType then gen.Emit OpCodes.Ldnull
        elif t = typeof<bool> || t = typeof<byte> || t = typeof<sbyte> ||
            t = typeof<char> || t = typeof<uint16> || t = typeof<int16> ||
            t = typeof<int32> || t = typeof<uint32> then
            gen.Emit OpCodes.Ldc_I4_0
        elif t = typeof<int64> || t = typeof<uint64> then
            gen.Emit OpCodes.Ldc_I4_0 ; gen.Emit OpCodes.Conv_I8
        elif t = typeof<single> then gen.Emit(OpCodes.Ldc_R4, 0.0f)
        elif t = typeof<double> then gen.Emit(OpCodes.Ldc_R8, 0.0)
        elif t = typeof<IntPtr> then gen.Emit OpCodes.Ldc_I4_0; gen.Emit OpCodes.Conv_I
        elif t = typeof<UIntPtr> then gen.Emit OpCodes.Ldc_I4_0; gen.Emit OpCodes.Conv_U
        else
            assert(not t.IsPrimitive)
            let lvar = gen.DeclareLocal t
            gen.Emit(OpCodes.Ldloca_S, lvar.LocalIndex)
            gen.Emit(OpCodes.Initobj, t)
            gen.Emit(OpCodes.Ldloc, lvar.LocalIndex)
        

    /// Emits a dynamic method that injects uninitialized values to supplied static method or ctor
    let emitUninitializedCtor<'DeclaringType> (ctor : MethodBase) =
        let builder (gen : ILGenerator) =
            for p in ctor.GetParameters() do ldDefaultValue gen p.ParameterType

            match ctor with
            | :? ConstructorInfo as c -> gen.Emit(OpCodes.Newobj, c)
            | :? MethodInfo as m when m.IsStatic -> gen.EmitCall(OpCodes.Call, m, null)
            | _ -> invalidMember ctor

            gen.Emit OpCodes.Ret

        let ctorId = sprintf "ctor-%O" ctor

        emitDynamicMethod<Func<'DeclaringType>> ctorId [||] typeof<'DeclaringType> builder

    /// Emits a dynamic method that gets the tag of supplied union case
    let emitUnionTagReader<'Union> (tagReader : MemberInfo) =
        let builder (gen : ILGenerator) =
            gen.Emit OpCodes.Ldarg_0
            if not typeof<'Union>.IsValueType then
                gen.Emit OpCodes.Ldind_Ref

            let m =
                match tagReader with
                | :? MethodInfo as m -> m
                | :? PropertyInfo as p -> p.GetGetMethod(true)
                | _ -> invalidMember tagReader

            if typeof<'Union>.IsValueType || m.IsStatic then 
                gen.EmitCall(OpCodes.Call, m, null)
            else 
                gen.EmitCall(OpCodes.Callvirt, m, null)

            gen.Emit OpCodes.Ret

        emitDynamicMethod<Getter<'Union, int>> "unionTagReader" [|typeof<'Union>.MakeByRefType()|] typeof<int> builder

    let emitISerializableCtor<'T when 'T :> ISerializable> (ctorInfo : ConstructorInfo) =
        let builder (gen : ILGenerator) =
            gen.Emit OpCodes.Ldarg_0
            gen.Emit OpCodes.Ldarg_1
            gen.Emit(OpCodes.Newobj, ctorInfo)
            gen.Emit OpCodes.Ret

        emitDynamicMethod<Func<SerializationInfo, StreamingContext, 'T>> "serializableCtor" 
            [|typeof<SerializationInfo>; typeof<StreamingContext>|] typeof<'T> builder
#endif

[<AutoOpen>]
module private ShapeTupleImpl =

    [<NoEquality; NoComparison>]
    type TupleInfo =
        { 
            Current : Type
            Fields : (MemberInfo * FieldInfo) []
            Nested : (FieldInfo * TupleInfo) option
        }

    let rec mkTupleInfo (t : Type) =
        if t.IsValueType then
            let fields = t.GetFields()
            let getField (f : FieldInfo) = f :> MemberInfo, f
            let fs, nested =
                if fields.Length = 8 then
                    let nestedField = fields.[7]
                    let nestedInfo = mkTupleInfo nestedField.FieldType
                    Array.map getField fields.[..6], Some(nestedField, nestedInfo)
                else
                    Array.map getField fields, None

            { Current = t ; Fields = fs ; Nested = nested }
        else
            let props = t.GetProperties()
            let fields = t.GetFields(BindingFlags.NonPublic ||| BindingFlags.Instance)
            let getField (p : PropertyInfo) =
                let field = fields |> Array.find(fun f -> f.Name = "m_" + p.Name)
                p :> MemberInfo, field

            let fs, nested =
                if props.Length = 8 then
                    let nestedField = fields.[7]
                    let nestedInfo = mkTupleInfo nestedField.FieldType
                    Array.map getField props.[..6], Some(nestedField, nestedInfo)
                else
                    Array.map getField props, None

            { Current = t ; Fields = fs ; Nested = nested }

    let gatherTupleMembers (tI : TupleInfo) =
        let rec aux (ctx : MemberInfo list) (tI : TupleInfo) = seq {
            for p,f in tI.Fields do
                yield p, f :> MemberInfo :: ctx |> List.rev |> List.toArray

            match tI.Nested with
            | Some (fI,n) -> yield! aux (fI :> MemberInfo :: ctx) n
            | None -> ()
        }

        aux [] tI

    let gatherNestedFields (tI : TupleInfo) =
        let rec aux fs (tI : TupleInfo) =
            match tI.Nested with
            | Some (fI,n) -> aux (fI :: fs) n
            | _ -> List.rev fs

        aux [] tI
        |> List.toArray

#if TYPESHAPE_EMIT
    open System.Reflection.Emit

    let emitCreateUninitializedTuple<'Tuple> (tupleInfo : TupleInfo) =
        let builder (gen : ILGenerator) =
            let rec emitCtor (tI : TupleInfo) =
                for (_,field) in tI.Fields do
                    ldDefaultValue gen field.FieldType

                match tI.Nested with
                | None -> ()
                | Some (_,nestedTupleInfo) -> emitCtor nestedTupleInfo

                let ctorInfo = tI.Current.GetConstructors().[0]
                gen.Emit(OpCodes.Newobj, ctorInfo)

            emitCtor tupleInfo
            gen.Emit OpCodes.Ret

        emitDynamicMethod<Func<'Tuple>> "tupleCtor" [||] typeof<'Tuple> builder

    let emitConstructorFunc<'Args, 'T> (arity : int) (ctorInfo : ConstructorInfo) =
        let builder (gen : ILGenerator) =
            match arity with
            | 0 -> ()
            | 1 -> gen.Emit OpCodes.Ldarg_0
            | _ ->
                let argsTupleInfo = mkTupleInfo typeof<'Args>
                let parentFields = new ResizeArray<FieldInfo>()
                let rec ldTupleElems (tI : TupleInfo) =
                    for (_,field) in tI.Fields do
                        gen.Emit OpCodes.Ldarg_0
                        for parentFld in parentFields do gen.Emit(OpCodes.Ldfld, parentFld)
                        gen.Emit(OpCodes.Ldfld, field)

                    match tI.Nested with
                    | None -> ()
                    | Some (field, nested) ->
                        parentFields.Add field
                        ldTupleElems nested

                ldTupleElems argsTupleInfo

            gen.Emit(OpCodes.Newobj, ctorInfo)
            gen.Emit OpCodes.Ret
                
        emitDynamicMethod<Func<'Args, 'T>> "constructorFunc" [|typeof<'Args>|] typeof<'T> builder
#endif

//-------------------------
// Member Shape Definitions

/// Identifies an instance member that defines a read-only value
/// in a class instance, typically a field or property
type IShapeReadOnlyMember =
    /// Human-readable member identifier
    abstract Label : string
    /// The actual System.Reflection.MemberInfo corresponding to member
    abstract MemberInfo : MemberInfo
    /// Type of value stored by member
    abstract Member : TypeShape
    /// True iff member is contained within a struct
    abstract IsStructMember : bool
    /// True iff member is public
    abstract IsPublic : bool

/// Identifies an instance member that defines a read-only value
/// in a class instance, typically a field or property
type IShapeReadOnlyMember<'DeclaringType> =
    inherit IShapeReadOnlyMember
    abstract Accept : IReadOnlyMemberVisitor<'DeclaringType, 'R> -> 'R

/// Identifies an instance member that defines a read-only value
/// in a class instance, typically a field or property
and ReadOnlyMember<'DeclaringType, 'MemberType> internal (label : string, memberInfo : MemberInfo, path : MemberInfo[]) =
    let isStructMember = isStructMember path
    let isPublicMember = isPublicMember memberInfo
#if TYPESHAPE_EMIT
    let projectFunc = emitGetter<'DeclaringType, 'MemberType> path
#endif

    /// Human-readable member identifier
    member _.Label = label
    /// The actual System.Reflection.MemberInfo corresponding to member
    member _.MemberInfo = memberInfo
    /// True iff member is contained within a struct
    member _.IsStructMember = isStructMember
    /// True iff member is public
    member _.IsPublic = isPublicMember
    /// Gets the current value from the given declaring type instance
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.Get (instance : 'DeclaringType) : 'MemberType =
#if TYPESHAPE_EMIT
        projectFunc.Invoke &instance
#else
        project path instance
#endif

    /// Gets the current value from the given declaring type instance
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.GetByRef (instance : inref<'DeclaringType>) : 'MemberType =
#if TYPESHAPE_EMIT
        projectFunc.Invoke &instance
#else
        project path instance
#endif
       
#if TYPESHAPE_EXPR
    /// Projects an instance to member of given value
    member _.GetExpr (instance : Expr<'DeclaringType>) =
        projectExpr<'DeclaringType, 'MemberType> path instance
#endif

    interface IShapeReadOnlyMember<'DeclaringType> with
        member s.Label = label
        member s.Member = shapeof<'MemberType> :> _
        member s.MemberInfo = memberInfo
        member s.IsStructMember = isStructMember
        member s.IsPublic = isPublicMember
        member s.Accept v = v.Visit s

and IReadOnlyMemberVisitor<'DeclaringType, 'R> =
    abstract Visit<'MemberType> : ReadOnlyMember<'DeclaringType, 'MemberType> -> 'R

//----------------------------
// Writable Member Definitions

/// Identifies an instance member that defines
/// a mutable value in a class instance, typically a field or property
type IShapeMember<'Record> =
    inherit IShapeReadOnlyMember<'Record>
    abstract Accept : IMemberVisitor<'Record,'R> -> 'R

/// Identifies an instance member that defines
/// a mutable value in a class instance, typically a field or property
and [<Sealed>] ShapeMember<'DeclaringType, 'MemberType> private (label : string, memberInfo : MemberInfo, path : MemberInfo[]) =
    inherit ReadOnlyMember<'DeclaringType, 'MemberType>(label, memberInfo, path)

#if TYPESHAPE_EMIT
    let injectFunc = emitSetter<'DeclaringType, 'MemberType> path
#else
    let isStructMember = isStructMember path
#endif

    /// Assigns value to the provided instance. NB this is a mutating operation
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.Set (instance : 'DeclaringType) (field : 'MemberType) : 'DeclaringType =
#if TYPESHAPE_EMIT
        let mutable instanceCpy = instance
        injectFunc.Invoke(&instanceCpy, field); instanceCpy
#else
        inject isStructMember path instance field
#endif

    /// Assigns value to the provided instance. NB this is a mutating operation
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.SetByRef (instance : byref<'DeclaringType>, field : 'MemberType) =
#if TYPESHAPE_EMIT
        injectFunc.Invoke(&instance, field)
#else
        instance <- inject isStructMember path instance field
#endif

#if TYPESHAPE_EXPR
    /// Injects a value to member of given instance
    member _.SetExpr (instance : Expr<'DeclaringType>) (field : Expr<'MemberType>) =
        injectExpr path instance field
#endif

    interface IShapeMember<'DeclaringType> with
        member s.Accept (v : IMemberVisitor<'DeclaringType, 'R>) = v.Visit s

and IMemberVisitor<'TRecord, 'R> =
    abstract Visit<'Field> : ShapeMember<'TRecord, 'Field> -> 'R

//-------------------------------
// Constructor Shapes

/// Identifies a constructor implementation shape
type IShapeConstructor =
    /// Denotes whether constructor is public
    abstract IsPublic : bool
    /// Denotes the arity of the constructor arguments
    abstract Arity : int
    /// ConstructorInfo instance
    abstract ConstructorInfo : ConstructorInfo
    // A tuple type encoding all arguments passed to the constuctor
    abstract Arguments : TypeShape

/// Identifies a constructor implementation shape
and IShapeConstructor<'DeclaringType> =
    inherit IShapeConstructor
    abstract Accept : IConstructorVisitor<'DeclaringType, 'R> -> 'R

/// Identifies a constructor implementation shape
and [<Sealed>] ShapeConstructor<'DeclaringType, 'CtorArgs> private (ctorInfo : ConstructorInfo, arity : int) =
#if TYPESHAPE_EMIT
    let ctorf = emitConstructorFunc<'CtorArgs, 'DeclaringType> arity ctorInfo
#else
    let valueReader = 
        match arity with
        | 0 -> fun _ -> [||]
        | 1 -> fun x -> [|x|]
        |_ -> FSharpValue.PreComputeTupleReader typeof<'CtorArgs>
#endif

    /// Creates an instance of declaring type with supplied constructor args
    member _.Invoke(args : 'CtorArgs) =
#if TYPESHAPE_EMIT
        ctorf.Invoke args
#else
        let args = valueReader args
        ctorInfo.Invoke args :?> 'DeclaringType
#endif


#if TYPESHAPE_EXPR
    /// Creates an instance of declaring type with supplied constructor args
    member _.InvokeExpr(args : Expr<'CtorArgs>) : Expr<'DeclaringType> =
        let exprArgs = 
            match arity with
            | 1 -> [args :> Expr]
            | _ -> [for i in 0 .. arity - 1 -> Expr.TupleGet(args, i)]

        Expr.Cast<'DeclaringType>(Expr.NewObject(ctorInfo, exprArgs))
#endif

    interface IShapeConstructor<'DeclaringType> with
        member _.IsPublic = ctorInfo.IsPublic
        member _.Arity = arity
        member _.ConstructorInfo = ctorInfo
        member _.Arguments = shapeof<'CtorArgs> :> _
        member c.Accept v = v.Visit c

and IConstructorVisitor<'CtorType, 'R> =
    abstract Visit<'CtorArgs> : ShapeConstructor<'CtorType, 'CtorArgs> -> 'R

//---------------------------
// Supplementary Member utils

[<AutoOpen>]
module private MemberUtils2 =
    let mkMemberUntyped<'Record> (label : string) (memberInfo : MemberInfo) (path : MemberInfo[]) =
        let memberType = 
            match path.[path.Length - 1] with
            | :? FieldInfo as fI -> fI.FieldType
            | :? PropertyInfo as pI -> pI.PropertyType
            | m -> invalidMember m

        let tyArgs = [|typeof<'Record> ; memberType|]
        let args = [|box label; box memberInfo; box path|]
        if isWriteableMember path then
            Activator.CreateInstanceGeneric<ShapeMember<_,_>>(tyArgs, args)
            :?> IShapeReadOnlyMember<'Record>
        else
            Activator.CreateInstanceGeneric<ReadOnlyMember<_,_>>(tyArgs, args) 
            :?> IShapeReadOnlyMember<'Record>
        
    let mkWriteMemberUntyped<'Record> (label : string) (memberInfo : MemberInfo) (path : MemberInfo[]) =
        match mkMemberUntyped<'Record> label memberInfo path with
        | :? IShapeMember<'Record> as wm -> wm
        | _ -> invalidOp <| sprintf "TypeShape internal error: Member '%O' is not writable" memberInfo

    let mkCtorUntyped<'Record> (ctorInfo : ConstructorInfo) =
        let argTypes = ctorInfo.GetParameters() |> Array.map (fun p -> p.ParameterType)
        let arity = argTypes.Length
        let argumentType =
            match arity with
            | 0 -> typeof<unit>
            | 1 -> argTypes.[0]
            | _ -> FSharpType.MakeStructTupleType(typeof<int>.Assembly, argTypes)

        Activator.CreateInstanceGeneric<ShapeConstructor<_,_>>([|typeof<'Record>; argumentType|], [|box ctorInfo; box arity|])
        :?> IShapeConstructor<'Record>

//--------------------
// Generic Tuple Shape

/// Denotes a specific System.Tuple shape
type IShapeTuple =
    /// Tuple element shape definitions
    abstract Elements : IShapeReadOnlyMember[]
    abstract Accept : ITupleVisitor<'R> -> 'R

and ITupleVisitor<'R> =
    abstract Visit : ShapeTuple<'Tuple> -> 'R 

/// Identifies a specific System.Tuple shape
and [<Sealed>] ShapeTuple<'Tuple> private () =
    let tupleInfo = mkTupleInfo typeof<'Tuple>

    let tupleElems = lazy(
        gatherTupleMembers tupleInfo
        |> Seq.mapi (fun i (pI, path) -> 
            let label = sprintf "Item%d" (i+1)
            mkWriteMemberUntyped<'Tuple> label pI path)
        |> Seq.toArray)

    let ctorf = 
#if TYPESHAPE_EMIT
        emitCreateUninitializedTuple<'Tuple> tupleInfo
#else
        if typeof<'Tuple>.IsValueType then Func<_>(fun () -> Unchecked.defaultof<'Tuple>)
        else
            let fieldStack = gatherNestedFields tupleInfo
            Func<_>(fun () ->
                let tuple = FormatterServices.GetUninitializedObject typeof<'Tuple>
                let mutable currentTuple = tuple
                for f in fieldStack do
                    let nestedTuple = FormatterServices.GetUninitializedObject f.FieldType
                    f.SetValue(currentTuple, nestedTuple)
                    currentTuple <- nestedTuple
                tuple :?> 'Tuple)
#endif

    member _.IsStructTuple = typeof<'Tuple>.IsValueType
    /// Tuple element shape definitions
    member _.Elements = tupleElems.Value
    /// Creates an uninitialized tuple instance of given type
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.CreateUninitialized() : 'Tuple = ctorf.Invoke()

#if TYPESHAPE_EXPR
    member _.CreateUninitializedExpr() : Expr<'Tuple> =
        if typeof<'Tuple>.IsValueType then
            Expr.Cast<'Tuple>(Expr.DefaultValue typeof<'Tuple>)
        else
            let values = tupleElems.Value |> Seq.map (fun e -> getDefaultValueExpr e.Member.Type) |> Seq.toList
            Expr.Cast<'Tuple>(Expr.NewTuple(values))
#endif

    interface IShapeTuple with
        member _.Elements = tupleElems.Value |> Array.map (fun e -> e :> _)
        member s.Accept v = v.Visit s

//---------------------
// F# Records

/// Denotes an F# record type
type IShapeFSharpRecord =
    abstract IsStructRecord : bool
    abstract IsAnonymousRecord : bool

    /// F# record field shapes
    abstract Fields : IShapeReadOnlyMember[]
    abstract Accept : IFSharpRecordVisitor<'R> -> 'R

/// Identifies an F# record type
and [<Sealed>] ShapeFSharpRecord<'Record> private () =
    // Warning: ugly hack -- should derive from FSharp.Reflection
    let isAnonymousRecord = typeof<'Record>.Name.StartsWith "<>f__AnonymousType"
    let ctorInfo = FSharpValue.PreComputeRecordConstructorInfo(typeof<'Record>, AllMembers)
    let props = FSharpType.GetRecordFields(typeof<'Record>, AllMembers)
    let fields = typeof<'Record>.GetFields(AllInstanceMembers)
    let mkRecordField (prop : PropertyInfo) =
        let backingField = fields |> Array.find (fun f -> f.Name = prop.Name + "@")
        mkWriteMemberUntyped<'Record> prop.Name prop [|backingField :> MemberInfo|]

#if TYPESHAPE_EMIT
    let ctorf = emitUninitializedCtor<'Record> ctorInfo
#else
    let ctorParams = props |> Array.map (fun p -> defaultOfUntyped p.PropertyType)
#endif

    let recordFields = lazy (Array.map mkRecordField props)

    /// True if an F# struct record
    member _.IsStructRecord = typeof<'Record>.IsValueType

    /// True if F# 4.6 anonyous records
    member _.IsAnonymousRecord = isAnonymousRecord

    /// F# record field shapes
    member _.Fields = recordFields.Value

    /// Creates an uninitialized instance for given record
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.CreateUninitialized() : 'Record =
#if TYPESHAPE_EMIT
        ctorf.Invoke()
#else
        ctorInfo.Invoke ctorParams :?> 'Record
#endif

#if TYPESHAPE_EXPR
    member _.CreateUninitializedExpr() : Expr<'Record> =
        if typeof<'Record>.IsValueType then <@ Unchecked.defaultof<'Record> @> else
        let values = props |> Seq.map (fun p -> getDefaultValueExpr p.PropertyType)
        Expr.Cast<'Record>(Expr.NewObject(ctorInfo, Seq.toList values))
#endif

    interface IShapeFSharpRecord with
        member _.IsStructRecord = typeof<'Record>.IsValueType
        member _.IsAnonymousRecord = isAnonymousRecord

        member _.Fields = recordFields.Value |> Array.map unbox
        member s.Accept v = v.Visit s

and IFSharpRecordVisitor<'R> =
    abstract Visit : ShapeFSharpRecord<'Record> -> 'R

//----------------------
// F# Unions

/// Denotes an F# union case shape
type IShapeFSharpUnionCase =
    /// Underlying FSharp.Reflection.UnionCaseInfo description
    abstract CaseInfo : UnionCaseInfo
    /// Field shapes for union case
    abstract Fields : IShapeReadOnlyMember[]

/// Denotes an F# union case shape
type [<Sealed>] ShapeFSharpUnionCase<'Union> private (uci : UnionCaseInfo) =
    let properties = uci.GetFields()
    let ctorInfo = FSharpValue.PreComputeUnionConstructorInfo(uci, AllMembers)
#if TYPESHAPE_EMIT
    let ctorf = emitUninitializedCtor<'Union> ctorInfo
#else
    let ctorParams = properties |> Array.map (fun p -> defaultOfUntyped p.PropertyType)
#endif

    let caseFields = lazy (
        match properties with
        | [||] -> [||]
        | _ ->
            let underlyingType = properties.[0].DeclaringType
            let allFields = underlyingType.GetFields(AllInstanceMembers)
            let mkUnionField (p : PropertyInfo) =
                let fieldInfo = allFields |> Array.find (fun f -> f.Name = "_" + p.Name || f.Name.ToLower() = p.Name.ToLower())
                mkWriteMemberUntyped<'Union> p.Name p [|fieldInfo|]

            Array.map mkUnionField properties)

    /// Underlying FSharp.Reflection.UnionCaseInfo description
    member _.CaseInfo = uci
    /// Number of fields in the particular union case
    member _.Arity = properties.Length
    /// Field shapes for union case
    member _.Fields = caseFields.Value

    /// Creates an uninitialized instance for specific union case
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.CreateUninitialized() : 'Union =
#if TYPESHAPE_EMIT
        ctorf.Invoke()
#else
        ctorInfo.Invoke(null, ctorParams) :?> 'Union
#endif

#if TYPESHAPE_EXPR
    member _.CreateUninitializedExpr() : Expr<'Union> =
        let fieldsExpr = properties |> Seq.map (fun p -> getDefaultValueExpr p.PropertyType)
        Expr.Cast<'Union>(Expr.Call(ctorInfo, Seq.toList fieldsExpr))
#endif

    interface IShapeFSharpUnionCase with
        member _.CaseInfo = uci
        member _.Fields = caseFields.Value |> Array.map (fun f -> f :> _)

/// Denotes an F# Union shape
type IShapeFSharpUnion =
    /// Case shapes for given union type
    abstract UnionCases : IShapeFSharpUnionCase[]
    abstract Accept : IFSharpUnionVisitor<'R> -> 'R

/// Denotes an F# Union shape
and [<Sealed>] ShapeFSharpUnion<'Union> private () =
    let ucis = lazy(
        FSharpType.GetUnionCases(typeof<'Union>, AllMembers)
        |> Array.map (fun uci -> 
            Activator.CreateInstanceGeneric<ShapeFSharpUnionCase<'Union>>([||],[|uci|]) 
            :?> ShapeFSharpUnionCase<'Union>))

#if TYPESHAPE_EMIT || TYPESHAPE_EXPR
    let tagReaderInfo = FSharpValue.PreComputeUnionTagMemberInfo(typeof<'Union>, AllMembers)
#endif

#if TYPESHAPE_EMIT
    let tagReader = emitUnionTagReader<'Union> tagReaderInfo
#else
    let tagReader = FSharpValue.PreComputeUnionTagReader(typeof<'Union>, AllMembers)
#endif

    let caseNames = lazy(ucis.Value |> Array.map (fun u -> u.CaseInfo.Name))

    member _.IsStructUnion = typeof<'Union>.IsValueType

    /// Case shapes for given union type
    member _.UnionCases = ucis.Value
    /// Gets the underlying tag id for given union instance
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.GetTag (union : 'Union) : int =
#if TYPESHAPE_EMIT
        tagReader.Invoke &union
#else
        tagReader union
#endif

    /// Gets the underlying tag id for given union instance
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.GetTagByRef (union : inref<'Union>) : int =
#if TYPESHAPE_EMIT
        tagReader.Invoke &union
#else
        tagReader union
#endif

    /// Looks the underlying tag id for given union case name using linear search.
    member _.GetTag (caseName : string) : int =
        let caseNames = caseNames.Value
        let n = caseNames.Length
        let mutable i = 0
        let mutable notFound = true
        while notFound && i < n do
            if caseNames.[i] = caseName then
                notFound <- false
            else
                i <- i + 1
        if notFound then raise <| KeyNotFoundException(sprintf "Union case: %A" caseName)
        i

#if TYPESHAPE_EXPR
    member _.GetTagExpr (union : Expr<'Union>) : Expr<int> =
        let expr =
            match tagReaderInfo with
            | :? MethodInfo as m when m.IsStatic -> Expr.Call(m, [union])
            | :? MethodInfo as m -> Expr.Call(union, m, [])
            | :? PropertyInfo as p -> Expr.PropertyGet(union, p)
            | _ -> invalidOp <| sprintf "Unexpected tag reader info %O" tagReaderInfo
        
        Expr.Cast<int> expr
#endif

    interface IShapeFSharpUnion with
        member _.UnionCases = ucis.Value |> Array.map (fun u -> u :> _)
        member s.Accept v = v.Visit s

and IFSharpUnionVisitor<'R> =
    abstract Visit : ShapeFSharpUnion<'U> -> 'R

//------------------------
// C# DTOs

/// Denotes a type that behaves like a mutable C# DTO:
/// Carries a parameterless constructor and settable properties
type IShapeCliMutable =
    /// Gettable and Settable properties for C# DTO
    abstract Properties : IShapeReadOnlyMember[]
    abstract Accept : ICliMutableVisitor<'R> -> 'R

/// Denotes a type that behaves like a C# record:
/// Carries a parameterless constructor and settable properties
and [<Sealed>] ShapeCliMutable<'Record> private (defaultCtor : ConstructorInfo) =
    let properties = lazy(
        typeof<'Record>.GetProperties(AllInstanceMembers)
        |> Seq.filter (fun p -> p.CanRead && p.CanWrite && p.GetIndexParameters().Length = 0)
        |> Seq.map (fun p -> mkWriteMemberUntyped<'Record> p.Name p [|p|])
        |> Seq.toArray)

#if TYPESHAPE_EMIT
    let ctor = emitUninitializedCtor<'Record> defaultCtor
#endif

    /// Creates an uninitialized instance for given C# record
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member _.CreateUninitialized() : 'Record =
#if TYPESHAPE_EMIT
        ctor.Invoke()
#else
        defaultCtor.Invoke [||] :?> 'Record
#endif

#if TYPESHAPE_EXPR
    /// Creates an uninitialized instance for given C# record
    member _.CreateUninitializedExpr() = 
        Expr.Cast<'Record>(Expr.NewObject(defaultCtor, []))
#endif

    /// Property shapes for C# record
    member _.Properties = properties.Value
    /// Gets the default constructor info defined in the type
    member _.DefaultCtorInfo = defaultCtor

    interface IShapeCliMutable with
        member _.Properties = properties.Value |> Array.map (fun p -> p :> _)
        member s.Accept v = v.Visit s

and ICliMutableVisitor<'R> =
    abstract Visit : ShapeCliMutable<'Record> -> 'R

//--------------------------
// Shape POCO

/// Denotes any .NET type that is either a class or a struct
type IShapePoco =
    /// True iff POCO is a struct
    abstract IsStruct : bool
    /// True iff POCO is a C# 9 record
    abstract IsCSharpRecord : bool
    /// Constructor shapes for the type
    abstract Constructors : IShapeConstructor[]
    /// Field shapes for the type
    abstract Fields : IShapeReadOnlyMember[]
    /// Property shapes for the type
    abstract Properties : IShapeReadOnlyMember[]
    abstract Accept : IPocoVisitor<'R> -> 'R

/// Denotes any .NET type that is either a class or a struct
and [<Sealed>] ShapePoco<'Poco> private () =
    let fields = lazy(
        gatherMembers (fun t -> t.GetFields(AllInstanceMembers ||| BindingFlags.FlattenHierarchy)) typeof<'Poco>
        |> Array.map (fun f -> mkWriteMemberUntyped<'Poco> f.Name f [|f|]))

    let isCSharpRecord =
        typeof<IEquatable<'Poco>>.IsAssignableFrom(typeof<'Poco>) &&
        let cloner = typeof<'Poco>.GetMethod("<Clone>$", BindingFlags.Instance ||| BindingFlags.Public, null, types = [||], modifiers = null) in
        not (isNull cloner) && cloner.IsVirtual

    let ctors = lazy(
        typeof<'Poco>.GetConstructors(AllInstanceMembers)
        // filter any ctors that accept byrefs or pointers
        |> Seq.filter (fun c -> c.GetParameters() |> Array.forall(fun p -> not <| isUnsupported p.ParameterType))
        |> Seq.map (fun c -> mkCtorUntyped<'Poco> c)
        |> Seq.toArray)

    let properties = lazy(
        typeof<'Poco>.GetProperties(AllInstanceMembers)
        |> Seq.filter (fun p -> p.CanRead)
        |> Seq.map (fun p -> mkMemberUntyped<'Poco> p.Name p [|p|])
        |> Seq.toArray)

    /// True iff POCO is a struct
    member _.IsStruct = typeof<'Poco>.IsValueType
    /// True iff POCO is a C# 9 record
    member _.IsCSharpRecord = isCSharpRecord
    /// Constructor shapes for the type
    member _.Constructors = ctors.Value
    /// Field shapes for the type
    member _.Fields = fields.Value
    /// Property shapes for the type
    member _.Properties = properties.Value

    /// Creates an uninitialized instance for POCO
    member _.CreateUninitialized() : 'Poco =
        if typeof<'Poco>.IsValueType then Unchecked.defaultof<'Poco>
        else FormatterServices.GetUninitializedObject(typeof<'Poco>) :?> 'Poco

#if TYPESHAPE_EXPR
    /// Creates an uninitialized instance for POCO
    member inline _.CreateUninitializedExpr() : Expr<'Poco> =
        <@ FormatterServices.GetUninitializedObject(typeof<'Poco>) :?> 'Poco @>
#endif

    interface IShapePoco with
        member _.Constructors = ctors.Value |> Array.map (fun c -> c :> _)
        member _.Fields = fields.Value |> Array.map (fun f -> f :> _)
        member _.Properties = properties.Value |> Array.map (fun p -> p :> _)
        member _.IsStruct = typeof<'Poco>.IsValueType
        member _.IsCSharpRecord = isCSharpRecord
        member s.Accept v = v.Visit s

and IPocoVisitor<'R> =
    abstract Visit : ShapePoco<'Poco> -> 'R

//-----------------------------
// Section: Shape ISerializable

type ISerializableVisitor<'R> =
    abstract Visit<'T when 'T :> ISerializable> : ShapeISerializable<'T> -> 'R

and IShapeISerializable =
    abstract CtorInfo : ConstructorInfo
    abstract Accept : ISerializableVisitor<'R> -> 'R

and ShapeISerializable<'T when 'T :> ISerializable> private () =
    let ctorTypes = [|typeof<SerializationInfo>; typeof<StreamingContext>|]
    let ctorInfo = typeof<'T>.GetConstructor(AllInstanceMembers, null, ctorTypes, [||])
    let getCtorInfo () =
        match ctorInfo with
        | null -> invalidOp <| sprintf "ISerializable constructor not available for type '%O'" typeof<'T>
        | ctor -> ctor

#if TYPESHAPE_EMIT
    let ctor = emitISerializableCtor<'T> (getCtorInfo())
#endif

    member _.CtorInfo = ctorInfo
    member _.Create(serializationInfo : SerializationInfo, streamingContext : StreamingContext) : 'T =
#if TYPESHAPE_EMIT
        ctor.Invoke(serializationInfo, streamingContext)
#else
        getCtorInfo().Invoke [| serializationInfo ; streamingContext |] :?> 'T
#endif

#if TYPESHAPE_EXPR
    member _.CreateExpr (serializationInfo : Expr<SerializationInfo>) (streamingContext : Expr<StreamingContext>) : Expr<'T> =
        Expr.Cast<'T>(Expr.NewObject(getCtorInfo(), [serializationInfo; streamingContext]))
#endif

    interface IShapeISerializable with
        member _.CtorInfo = ctorInfo
        member s.Accept v = v.Visit<'T> s

//--------------------------------------
// Section: TypeShape active recognizers

[<RequireQualifiedAccess>]
module Shape =

    let private SomeU = Some() // avoid allocating all the time
    let inline private test<'T> (s : TypeShape) =
        match s with
        | :? TypeShape<'T> -> SomeU
        | _ -> None

    // ----------
    // Primitives

    let (|Bool|_|) s = test<bool> s
    let (|Byte|_|) s = test<byte> s
    let (|SByte|_|) s = test<sbyte> s
    let (|Int16|_|) s = test<int16> s
    let (|Int32|_|) s = test<int32> s
    let (|Int64|_|) s = test<int64> s
    let (|IntPtr|_|) s = test<nativeint> s
    let (|UInt16|_|) s = test<uint16> s
    let (|UInt32|_|) s = test<uint32> s
    let (|UInt64|_|) s = test<uint64> s
    let (|UIntPtr|_|) s = test<unativeint> s
    let (|Single|_|) s = test<single> s
    let (|Double|_|) s = test<double> s
    let (|Char|_|) s = test<char> s
    let (|Primitive|_|) (s:TypeShape) =
        if s.Type.IsPrimitive then SomeU
        else None

#if !TYPESHAPE_DISABLE_BIGINT
    let (|BigInt|_|) s = test<bigint> s
#endif

    let (|String|_|) s = test<string> s
    let (|Guid|_|) s = test<Guid> s
    let (|Uri|_|) s = test<Uri> s
    let (|Decimal|_|) s = test<decimal> s
    let (|TimeSpan|_|) s = test<TimeSpan> s
    let (|DateTime|_|) s = test<DateTime> s
    let (|DateTimeOffset|_|) s = test<DateTimeOffset> s
    let (|Unit|_|) s = test<unit> s
    let (|FSharpUnit|_|) s = test<unit> s
    let (|ByteArray|_|) s = test<byte []> s
        
    /// Recognizes any type that is a .NET enumeration
    let (|Enum|_|) (s : TypeShape) = 
        match s.ShapeInfo with
        | Enum(e,u) ->
            Activator.CreateInstanceGeneric<ShapeEnum<BindingFlags, int>> [|e;u|]
            :?> IShapeEnum 
            |> Some
        | _ -> None

    /// Recognizes any type that satisfies the F# `equality` constraint
    let (|Equality|_|) (s : TypeShape) =
        // Since equality & comparison constraints are not contained
        // in reflection metadata, we need to separately determine 
        // whether they are satisfied
        // c.f. Section 5.2.10 of the F# Spec
        let rec isEqualityConstraint (stack:Type list) (t:Type) =
            if stack |> List.exists ((=) t) then true // recursive paths resolve to true always
            elif FSharpType.IsUnion(t, AllMembers) then 
                if t.IsValueType then
                    t.GetProperties(AllMembers)
                    |> Seq.filter (fun p -> p.Name <> "Tag")
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isEqualityConstraint (t :: stack))

                elif t.ContainsAttr<NoEqualityAttribute>(true) then false
                elif t.ContainsAttr<CustomEqualityAttribute>(true) then true
                else
                    FSharpType.GetUnionCases(t, AllMembers)
                    |> Seq.collect (fun uci -> uci.GetFields())
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isEqualityConstraint (t :: stack))

            elif t.ContainsAttr<NoEqualityAttribute>(false) then false
            elif FSharpType.IsRecord(t, AllMembers) then
                if t.ContainsAttr<CustomEqualityAttribute>(true) then false
                else
                    FSharpType.GetRecordFields(t, AllMembers)
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isEqualityConstraint (t :: stack))

            elif FSharpType.IsTuple t then
                FSharpType.GetTupleElements t
                |> Seq.distinct
                |> Seq.forall (isEqualityConstraint (t :: stack))

            elif FSharpType.IsFunction t then false
            elif t.IsArray then
                isEqualityConstraint (t :: stack) (t.GetElementType())
            else
                true

        if isEqualityConstraint [] s.Type then
            Activator.CreateInstanceGeneric<ShapeEquality<_>> [|s.Type|]
            :?> IShapeEquality
            |> Some
        else
            None

    /// Recognizes any type that satisfies the F# `comparison` constraint
    let (|Comparison|_|) (s : TypeShape) =
        // Since equality & comparison constraints are not contained
        // in reflection metadata, we need to separately determine 
        // whether they are satisfied
        // c.f. Section 5.2.10 of the F# Spec
        let rec isComparisonConstraint (stack:Type list) (t:Type) =
            if t = typeof<IntPtr> || t = typeof<UIntPtr> then true
            elif stack |> List.exists ((=) t) then true // recursive paths resolve to true always
            elif FSharpType.IsUnion(t, AllMembers) then 
                if t.IsValueType then
                    t.GetProperties(AllMembers)
                    |> Seq.filter (fun p -> p.Name <> "Tag")
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isComparisonConstraint (t :: stack))

                elif t.ContainsAttr<NoComparisonAttribute>(true) then false 
                elif t.ContainsAttr<CustomComparisonAttribute>(true) then true
                else
                    FSharpType.GetUnionCases(t, AllMembers)
                    |> Seq.collect (fun uci -> uci.GetFields())
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isComparisonConstraint (t :: stack))

            elif t.ContainsAttr<NoComparisonAttribute>(false) then false
            elif FSharpType.IsRecord(t, AllMembers) then
                if t.ContainsAttr<CustomComparisonAttribute>(true) then false
                else
                    FSharpType.GetRecordFields(t, AllMembers)
                    |> Seq.map (fun p -> p.PropertyType)
                    |> Seq.distinct
                    |> Seq.forall (isComparisonConstraint (t :: stack))

            elif FSharpType.IsTuple t then
                FSharpType.GetTupleElements t
                |> Seq.distinct
                |> Seq.forall (isComparisonConstraint (t :: stack))

            elif t.IsArray then
                isComparisonConstraint (t :: stack) (t.GetElementType())
            else
                isInterfaceAssignableFrom typeof<IComparable> t

        if isComparisonConstraint [] s.Type then
            Activator.CreateInstanceGeneric<ShapeComparison<_>> [|s.Type|]
            :?> IShapeComparison
            |> Some
        else
            None

    /// Identifies whether shape satisfies the 'struct', 'not struct' or 'nullable' constraint
    let (|Struct|NotStruct|Nullable|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<Nullable<_>> ->
            let shape = Activator.CreateInstanceGeneric<ShapeNullable<_>>(ta) :?> IShapeNullable
            Nullable shape

        | _ when s.Type.IsValueType ->
            let instance = Activator.CreateInstanceGeneric<ShapeStruct<_>> [|s.Type|] :?> IShapeStruct
            Struct instance
        | _ ->
            let instance = Activator.CreateInstanceGeneric<ShapeNotStruct<_>> [|s.Type|] :?> IShapeNotStruct
            NotStruct instance

    /// Recognizes shapes that carry a parameterless constructor
    let (|DefaultConstructor|_|) (shape : TypeShape) =
        match shape.Type.GetConstructor(BindingFlags.Public ||| BindingFlags.Instance, null, [||], [||]) with
        | null -> None
        | _ -> 
            Activator.CreateInstanceGeneric<ShapeDefaultConstructor<_>>([|shape.Type|]) 
            :?> IShapeDefaultConstructor
            |> Some

    /// Recognizes shapes that are instances of System.Collections.Generic.KeyValuePair<_,_>
    let (|KeyValuePair|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<KeyValuePair<_,_>> ->
            Activator.CreateInstanceGeneric<ShapeKeyValuePair<_,_>>(ta)
            :?> IShapeKeyValuePair
            |> Some
        | _ ->
            None

    /// Recognizes shapes that are instances of System.Collections.Generic.Dictionary<_,_>
    let (|Dictionary|_|) (s : TypeShape) = 
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<Dictionary<_,_>> ->
            Activator.CreateInstanceGeneric<ShapeDictionary<_,_>>(ta)
            :?> IShapeDictionary
            |> Some
        | _ ->
            None

    /// Recognizes shapes that are instances of System.Collections.Generic.HashSet<_>
    let (|HashSet|_|) (s : TypeShape) = 
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<HashSet<_>> ->
            Activator.CreateInstanceGeneric<ShapeHashSet<_>>(ta)
            :?> IShapeHashSet
            |> Some
        | _ ->
            None

    /// Recognizes shapes that are instances of System.Collections.Generic.List<_>
    let (|ResizeArray|_|) (s : TypeShape) = 
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<ResizeArray<_>> ->
            Activator.CreateInstanceGeneric<ShapeResizeArray<_>>(ta)
            :?> IShapeResizeArray
            |> Some
        | _ ->
            None

    /// Recognizes shapes that inherit from System.Delegate
    let (|Delegate|_|) (s : TypeShape) =
        if typeof<System.Delegate>.IsAssignableFrom s.Type then
            Activator.CreateInstanceGeneric<ShapeDelegate<_>>([|s.Type|])
            :?> IShapeDelegate
            |> Some
        else
            None

    /// Recognizes shapes that inherit from System.Exception
    let (|Exception|_|) (s : TypeShape) =
        if typeof<System.Exception>.IsAssignableFrom s.Type then
            let isFSharpExn = FSharpType.IsExceptionRepresentation(s.Type, AllMembers)
            Activator.CreateInstanceGeneric<ShapeException<_>>([|s.Type|], [|isFSharpExn|])
            :?> IShapeException
            |> Some
        else
            None

    /// Recognizes shapes that implement ISerializable
    let (|ISerializable|_|) (shape : TypeShape) =
        if typeof<ISerializable>.IsInterfaceAssignableFrom shape.Type then
            Activator.CreateInstanceGeneric<ShapeISerializable<_>>([|shape.Type|])
            :?> IShapeISerializable
            |> Some
        else
            None

    /// Recognizes shapes that are .NET arrays
    let (|Array|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | TypeShapeInfo.Array(et,rk) ->
            Activator.CreateInstanceGeneric<ShapeArray<_>>([|et|], [|box rk|])
            :?> IShapeArray
            |> Some
        | _ ->
            None

    let (|SystemArray|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | TypeShapeInfo.Array(et, rk) ->
            Activator.CreateInstanceGeneric<ShapeSystemArray<_>>([|s.Type|], [|box et; box rk|])
            :?> IShapeSystemArray
            |> Some
        | _ ->
            None

    /// Recognizes shapes of F# list types
    let (|FSharpList|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<_ list> ->
            Activator.CreateInstanceGeneric<ShapeFSharpList<_>>(ta)
            :?> IShapeFSharpList
            |> Some
        | _ -> None

    /// Recognizes shapes of F# option types
    let (|FSharpOption|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<_ option> ->
            Activator.CreateInstanceGeneric<ShapeFSharpOption<_>>(ta)
            :?> IShapeFSharpOption
            |> Some
        | _ -> None

    /// Recognizes shapes of F# ref types
    let (|FSharpRef|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<_ ref> ->
            Activator.CreateInstanceGeneric<ShapeFSharpRef<_>>(ta)
            :?> IShapeFSharpRef
            |> Some
        | _ -> None

    /// Recognizes shapes of F# set types
    let (|FSharpSet|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<Set<_>> ->
            Activator.CreateInstanceGeneric<ShapeFSharpSet<_>>(ta)
            :?> IShapeFSharpSet
            |> Some
        | _ -> None

    /// Recognizes shapes of F# map types
    let (|FSharpMap|_|) (s : TypeShape) =
        match s.ShapeInfo with
        | Generic(td,ta) when td = typedefof<Map<_,_>> -> 
            Activator.CreateInstanceGeneric<ShapeFSharpMap<_,_>>(ta)
            :?> IShapeFSharpMap
            |> Some
        | _ -> None

    /// Recognizes shapes of F# function types
    let (|FSharpFunc|_|) (s : TypeShape) =
        if FSharpType.IsFunction s.Type then
            let d,c = FSharpType.GetFunctionElements s.Type
            Activator.CreateInstanceGeneric<ShapeFSharpFunc<_,_>> [|d;c|]
            :?> IShapeFSharpFunc
            |> Some
        else None

    /// Recognizes shapes that implement System.Collections.Generic.ICollection<_>
    let (|Collection|_|) (s : TypeShape) =
        match s.Type.GetInterface("ICollection`1") with
        | null ->
            match s.ShapeInfo with
            | Generic(td,ta) when td = typedefof<ICollection<_>> ->
                Activator.CreateInstanceGeneric<ShapeCollection<_,_>> [|s.Type; ta.[0]|]
                :?> IShapeCollection
                |> Some
            | _ -> None
        | iface ->
            let args = iface.GetGenericArguments()
            Activator.CreateInstanceGeneric<ShapeCollection<_,_>> [|s.Type; args.[0]|]
            :?> IShapeCollection
            |> Some

    /// Recognizes shapes that implement System.Collections.Generic.IEnumerable<_>
    let (|Enumerable|_|) (s : TypeShape) =
        match s.Type.GetInterface("IEnumerable`1") with
        | null ->
            match s.ShapeInfo with
            | Generic(td,ta) when td = typedefof<IEnumerable<_>> ->
                Activator.CreateInstanceGeneric<ShapeEnumerable<_,_>> [|s.Type; ta.[0]|]
                :?> IShapeEnumerable
                |> Some
            | _ -> None
        | iface ->
            let args = iface.GetGenericArguments()
            Activator.CreateInstanceGeneric<ShapeEnumerable<_,_>> [|s.Type; args.[0]|]
            :?> IShapeEnumerable
            |> Some

    /// Recognizes shapes that are F# records
    let (|FSharpRecord|_|) (s : TypeShape) =
        if FSharpType.IsRecord(s.Type, AllMembers) then
            Activator.CreateInstanceGeneric<ShapeFSharpRecord<_>>([|s.Type|], [||])
            :?> IShapeFSharpRecord
            |> Some
        else
            None

    /// Recognizes shapes that are F# unions
    let (|FSharpUnion|_|) (s : TypeShape) =
        if FSharpType.IsUnion(s.Type, AllMembers) then
            if s.Type.IsValueType && fsharpCoreRuntimeVersion < fsharpCore41Version then
                sprintf "TypeShape error: FSharp.Core Runtime %A does not support struct unions. %A or later is required"
                    fsharpCoreRuntimeVersion fsharpCore41Version
                |> invalidOp

            Activator.CreateInstanceGeneric<ShapeFSharpUnion<_>>([|s.Type|], [||])
            :?> IShapeFSharpUnion
            |> Some
        else
            None

    /// Recognizes shapes that are System.Tuple instances of arbitrary arity
    let (|Tuple|_|) (s : TypeShape) =
        if FSharpType.IsTuple s.Type then
            Activator.CreateInstanceGeneric<ShapeTuple<_>>([|s.Type|], [||])
            :?> IShapeTuple
            |> Some
        else
            None

    /// Recognizes shapes that look like C# record classes
    /// They are classes with parameterless constructors and settable properties
    let (|CliMutable|_|) (s : TypeShape) =
        if s.Type.IsAbstract then None else
        match s.Type.GetConstructor(AllInstanceMembers, null, [||], [||]) with
        | null -> None
        | ctor -> 
            Activator.CreateInstanceGeneric<ShapeCliMutable<_>>([|s.Type|], [|ctor|])
            :?> IShapeCliMutable
            |> Some

    /// Recognizes POCO shapes, .NET types that are either classes, structs or C# records types
    let (|Poco|_|) (s : TypeShape) =
        let isPocoClass (t : Type) = 
            t.IsClass && 
            not t.IsAbstract && 
            not t.IsMarshalByRef

        let isPocoStruct (t : Type) =
            t.IsValueType &&
            not t.IsPrimitive &&
            not t.IsEnum

        if isPocoClass s.Type || isPocoStruct s.Type then
            let isNullable () =
                match s.ShapeInfo with
                | Generic(td,_) -> td = typedefof<Nullable<_>>
                | _ -> false

            let hasPointers () =
                s.Type.GetMembers AllInstanceMembers
                |> Seq.choose (function 
                    | :? PropertyInfo as p -> Some p.PropertyType
                    | :? FieldInfo as f -> Some f.FieldType
                    | _ -> None)
                |> Seq.exists isUnsupported

            if isNullable() || hasPointers() then None // do not recognize if type has pointer fields
            else
                Activator.CreateInstanceGeneric<ShapePoco<_>>([|s.Type|], [||])
                :?> IShapePoco
                |> Some
        else
            None
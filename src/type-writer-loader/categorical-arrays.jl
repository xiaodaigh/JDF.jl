using DataAPI

using CategoricalArrays: CategoricalVector, CategoricalArray, CategoricalPool

function compress_then_write(b::CategoricalVector{T,IntType}, io) where {T, IntType<:Integer}
    compress_refs = compress_then_write(b.refs, io)
    compress_poolindex = compress_then_write(DataAPI.levels(b), io)

    (
        type = CategoricalVector,
        refs = compress_refs,
        poolindex = compress_poolindex,
        ordered = b.pool.ordered,
    )
end

# function column_loader(::Type{CategoricalVector{Union{Missing, T}, I}}, io, metadata) where {T, I}
#     println("got here1")
#     refs_meta = metadata.refs
#     pi_meta = metadata.poolindex
#     ref = column_loader(refs_meta.type, io, refs_meta)
#     poolindex = column_loader(pi_meta.type, io, pi_meta)

#     return CategoricalArray{pi_meta.type,1}(
#         ref,
#         CategoricalPool{eltype(poolindex),eltype(ref)}(Array(poolindex), metadata.ordered),
#     )
# end

function column_loader(::Type{CategoricalVector}, io, metadata)
    refs_meta = metadata.refs
    pi_meta = metadata.poolindex
    ref = column_loader(refs_meta.type, io, refs_meta)
    poolindex = column_loader(pi_meta.type, io, pi_meta)

    # this checks for missing in the values which would be represented by ref = 0
    if any(==(0), ref)
        return CategoricalArray{Union{pi_meta.type, Missing},1}(
            ref,
            CategoricalPool{eltype(poolindex),eltype(ref)}(Array(poolindex), metadata.ordered),
        )
    else
        # no missing in the values, just return
        return CategoricalArray{pi_meta.type,1}(
            ref,
            CategoricalPool{eltype(poolindex),eltype(ref)}(Array(poolindex), metadata.ordered),
        )
    end
end

if false
    # # compress_then_write-cate.jl
    # using Revise
    # using JDF
    # using DataFrames, Debugger
    # b = categorical(["a", "b", "a", "c"])
    # io = open("c:/data/test.io", "w")
    # metadata = compress_then_write(a, io)
    # close(io)
    #
    # io = open("c:/data/test.io", "r")
    # aa = column_loader(CategoricalVector, io, metadata)
    # close(io)
    #
    # df = DataFrame(a = a)
    #
    # savejdf("c:/data/pls_del.jdf", df)
    #
    # loadjdf("c:/data/pls_del.jdf")
end

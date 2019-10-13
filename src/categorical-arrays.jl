# # compress_then_write-cate.jl
# using Revise
# using JDF
# using DataFrames, Debugger

compress_then_write(b::CategoricalVector{T, IntType}, io) where {T, IntType <: Integer} = begin
    compress_refs = compress_then_write(b.refs, io)
    compress_poolindex = compress_then_write(b.pool.index, io)

    (type = CategoricalVector, refs = compress_refs, poolindex = compress_poolindex, ordered = b.pool.ordered)
end

column_loader(b::Type{CategoricalVector}, io, metadata) = begin
    refs_meta = metadata.refs
    pi_meta = metadata.poolindex
    ref = column_loader(refs_meta.type, io, refs_meta)
    poolindex = column_loader(pi_meta.type, io, pi_meta)
    CategoricalArray{pi_meta.type, 1}(ref, CategoricalPool(Array(poolindex), metadata.ordered))
end

if false
a = categorical(["a", "b", "a", "c"])
io = open("c:/data/test.io", "w")
metadata = compress_then_write(a, io)
close(io)

io = open("c:/data/test.io", "r")
aa = column_loader(CategoricalVector, io, metadata)
close(io)

df = DataFrame(a = a)

savejdf("c:/data/pls_del.jdf", df)

loadjdf("c:/data/pls_del.jdf")
end

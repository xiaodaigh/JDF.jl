compress_then_write(_, b::StringVector{T}, io) where {T} = compress_then_write(b, io)

compress_then_write(b::StringVector{T}, io) where {T} = begin
    (
     metadata = [(eltype(getfield(b, n)), write(io, Blosc.compress(getfield(b, n)))) for n in fieldnames(typeof(b))],
     type = typeof(b),
    )
end

column_loader(::Type{StringVector{T}}, io, metadata) where {T} = begin
    # uncompress
    args = Vector[]

    # assign the buffer once
    buffer = Vector{UInt8}(undef, maximum(x -> x[2], metadata.metadata))

    for (elm_type, compressed_bytes) in metadata.metadata
        readbytes!(io, buffer, compressed_bytes)
        push!(args, Blosc.decompress(elm_type, buffer))
    end
    metadata.type(args...)
end

if false
using Revise
using WeakRefStrings, Blosc, JDF, DataFrames

a = StringVector(["a", "b", "a", missing, "c"])
io = open("c:/data/test.io", "w")
metadata = compress_then_write(a, io)
close(io)

io = open("c:/data/test.io", "r")
aa = column_loader(StringVector{String}, io, metadata)
close(io)

df = DataFrame(a = a)

savejdf("c:/data/pls_del.jdf", df)

loadjdf("c:/data/pls_del.jdf")
end

compress_then_write(b::StringVector{T}, io) where {T} = begin

    #println("ok")
    # bufferc = Blosc.compress(b.buffer )
    # buffer_meta  = (eltype(b.buffer) ,  write(io, bufferc))
    #lengths_meta = (eltype(b.lengths),  write(io, Blosc.compress(b.lengths)))
    #offsets_meta = (eltype(b.offsets),  write(io, Blosc.compress(b.offsets)))
    bbc = Blosc.compress(b.buffer)
    len1 = length(bbc)
    len2 = write(io, bbc)
    (b.buffer, bbc, len1, len2)
    # (
    #  #metadata = (buffer = buffer_meta, lengths = lengths_meta, offsets = offsets_meta),
    #  metadata = (buffer = (type=UInt8, len = res)),
    #  type = typeof(b),
    #  bufferc = bbc
    # )
end

column_loader(::Type{StringVector{T}}, io, metadata) where {T} = begin
    args = Any[]

    # assign the buffer once
    #buffer = Vector{UInt8}(undef, maximum(x -> x[2], metadata.metadata))

    buffer = Vector{UInt8}(undef, metadata.metadata.buffer[2])
    return metadata.metadata.buffer
    readbytes!(io, buffer, metadata.metadata.buffer[2])
    return Blosc.decompress(metadata.metadata.buffer[1], buffer)
    # for (elm_type, compressed_bytes) in metadata.metadata
    #     println(elm_type, compressed_bytes)
    #     buffer = Vector{UInt8}(undef, compressed_bytes)
    #     readbytes!(io, buffer, compressed_bytes)
    #     println(length(buffer))
    #     res = Blosc.decompress(elm_type, buffer)
    #     return res
    #     push!(args, res)
    # end
    return args
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

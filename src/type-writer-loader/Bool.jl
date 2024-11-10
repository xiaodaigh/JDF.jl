function column_loader(T::Type{Bool}, io, metadata)
    # Bool are saved as UInt8
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, T, io, metadata)
end

function column_loader!(buffer, T::Type{Bool}, io, metadata)
    # Bool are saved as UInt8
    readbytes!(io, buffer, metadata.len)
    res = Blosc.decompress(UInt8, buffer)
    Bool.(res)
end

function compress_then_write(b::Vector{Bool}, io)
    b8 = UInt8.(b)
    bbc = Blosc.compress(b8)
    write(io, bbc)
    return (len = length(bbc), type = Bool)
end

column_loader(T::Type{Bool}, io, metadata) = begin
# Bool are saved as UInt8
    buffer = Vector{UInt8}(undef, metadata.len)
    readbytes!(io, buffer, metadata.len)
    Bool.(Blosc.decompress(UInt8, buffer))
end

column_loader!(buffer, T::Type{Bool}, io, metadata) = begin
    # Bool are saved as UInt8
    read!(io, buffer)
    res = Blosc.decompress(UInt8, buffer)
    Bool.(res)
end

compress_then_write(b::Vector{Bool}, io) = begin
    b8 = UInt8.(b)
    bbc = Blosc.compress(b8)
    write(io, bbc)
    return (len = length(bbc), type = Bool)
end

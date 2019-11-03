"""
Load data from file using metadata
"""
column_loader(t::Type{T}, io, metadata) where {T} = begin
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, t, io, metadata)
end

# load bytes bytes from io decompress into type
column_loader!(buffer, ::Type{T}, io, metadata) where {T} = begin
    readbytes!(io, buffer, metadata.len)
    return Blosc.decompress(T, buffer)
end

"""
Load data from file using metadata
"""
function column_loader(t::Type{T}, io, metadata) where {T}
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, t, io, metadata)
end

# load bytes from io decompress into type
function column_loader!(buffer, ::Type{T}, io, metadata) where {T}
    readbytes!(io, buffer, metadata.len)
    return Blosc.decompress(T, buffer)
end

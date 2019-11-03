# the generic dispatch
blosc_compress(b::CC, io) where CC <: CSV.Column = begin
    blosc_compress(Vector(b), io)
end

blosc_compress(b::AbstractVector{T}, io) where T = begin
    #return eltype(b), Vector(b)
    blosc_compress(Vector(b), io)
end

# the generic dispatch for T where is isbits
blosc_compress(b::Vector{T}, io) where T = begin
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end


"""
Decompress
"""
blosc_decompress(t::Type{T}, io, metadata) where {T} = begin
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, t, io, metadata)
end

# load bytes bytes from io decompress into type
blosc_decompress!(buffer, ::Type{T}, io, metadata) where {T} = begin
    readbytes!(io, buffer, metadata.len)
    return Blosc.decompress(T, buffer)
end

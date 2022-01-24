function compress_then_write(b::AbstractVector{T}, io) where {T}
    compress_then_write(Vector(b), io)
end

# the generic dispatch for T where is isbits
function compress_then_write(b::Vector{T}, io) where {T}
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end

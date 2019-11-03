# the generic dispatch
compress_then_write(b::CC, io) where CC <: CSV.Column = begin
    compress_then_write(Vector(b), io)
end

compress_then_write(b::AbstractVector{T}, io) where T = begin
    #return eltype(b), Vector(b)
    compress_then_write(Vector(b), io)
end

# the generic dispatch for T where is isbits
compress_then_write(b::Vector{T}, io) where T = begin
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end

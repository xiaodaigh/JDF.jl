# the generic dispatch
compress_then_write(b::AbstractArray, io) = compress_then_write(eltype(b), Vector(b), io)

# the generic dispatch for T where is isbits
compress_then_write(T, b, io) = begin
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end

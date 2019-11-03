# save symbols
compress_then_write(b::Vector{Char}, io) = begin
    meta = compress_then_write(Int.(b), io)
    (type = Char, len = meta.len, metadata = meta)
end

# load a Symbol column
column_loader(::Type{Char}, io, metadata) = begin
    strs = column_loader(Int, io, metadata.metadata)
    Char.(strs)
end

column_loader!(_, ::Type{Char}, io, metadata) = begin
    column_loader(Char, io, metadata)
end

some_elm(::Type{Char}) = 'J'

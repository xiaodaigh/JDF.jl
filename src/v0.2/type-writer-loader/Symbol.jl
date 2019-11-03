# save symbols
compress_then_write(b::Vector{Symbol}, io) = begin
    string_meta = compress_then_write(String.(b), io)
    (type = Symbol, len = string_meta.len, metadata = string_meta)
end

# load a Symbol column
column_loader(::Type{Symbol}, io, metadata) = begin
    strs = column_loader(String, io, metadata.metadata)
    Symbol.(strs)
end

column_loader!(_, ::Type{Symbol}, io, metadata) = begin
    column_loader(Symbol, io, metadata)
end

some_elm(::Type{Symbol}) = :JDF

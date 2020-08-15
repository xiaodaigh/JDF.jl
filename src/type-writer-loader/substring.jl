
function compress_then_write(b::Vector{SubString{String}}, io)
    @warn "JDF is write a SubString vector. When loaded back it will be a String vector not a SubString vector"
    compress_then_write(String.(b), io)
end

function column_loader(::Type{SubString{String}}, io, metadata)
    @warn "JDF is loading SubString vector. It will be loaded as a String vector not a SubString vector"
    column_loader(String, io, metadata)
end
# save symbols
# compress_then_write(b::Vector{DateTime}, io) = begin
#     meta = compress_then_write(map(x->x.instant, b), io)
#     (type = DateTime, len = meta.len, metadata = meta)
# end
#
# # load a Symbol column
# column_loader(::Type{DateTime}, io, metadata) = begin
#     strs = column_loader(meta.type, io, metadata.metadata)
#     DateTime.(strs)
# end
#
# column_loader!(_, ::Type{DateTime}, io, metadata) = begin
#     column_loader(DateTime, io, metadata)
# end
#
some_elm(::Type{DateTime}) = DateTime(1)

# turns out DateTime is BLosc Compressable

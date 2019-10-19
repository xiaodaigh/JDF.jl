compress_then_write(b::StringVector{T}, io) where {T} = begin

    buffer_meta  = (type=eltype(b.buffer) ,  len=write(io, Blosc.compress(b.buffer)))
    offsets_meta = (type=eltype(b.offsets),  len=write(io, Blosc.compress(b.offsets)))
    lengths_meta = (type=eltype(b.lengths),  len=write(io, Blosc.compress(b.lengths)))

    (
        metadata = (buffer = buffer_meta, offsets = offsets_meta, lengths = lengths_meta),
        type = typeof(b),
    )
end

column_loader(::Type{StringVector{T}}, io, metadata) where {T} = begin
    buffer  = column_loader(metadata.metadata.buffer.type , io, metadata.metadata.buffer)
    offsets = column_loader(metadata.metadata.offsets.type, io, metadata.metadata.offsets)
    lengths = column_loader(metadata.metadata.lengths.type, io, metadata.metadata.lengths)

    metadata.type(buffer, offsets, lengths)
end

# tests at test/test-stringarray.jl

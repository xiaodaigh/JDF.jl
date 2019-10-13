column_loader(t::Type{T}, io, metadata) where {T} = begin
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, t, io, metadata)
end

# load bytes bytes from io decompress into type
column_loader!(buffer, ::Type{T}, io, metadata) where {T} = begin
    readbytes!(io, buffer, metadata.len)
    return Blosc.decompress(T, buffer)
end

column_loader(T::Type{Bool}, io, metadata) = begin
# Bool are saved as UInt8
    buffer = Vector{UInt8}(undef, metadata.len)
    readbytes!(io, buffer, metadata.len)
    Bool.(Blosc.decompress(UInt8, buffer))
end

column_loader!(buffer, T::Type{Bool}, io, metadata) = begin
    # Bool are saved as UInt8
    read!(io, buffer)
    res = Blosc.decompress(UInt8, buffer)
    Bool.(res)
end

column_loader!(buffer, ::Type{Union{Missing,T}}, io, metadata) where {T} = begin
    # read the content
    Tmeta = metadata.Tmeta

    t_pre = column_loader!(buffer, Tmeta.type, io, Tmeta) |> allowmissing
    #t = t_pre
    # read the missings as bool
    m = column_loader(Bool, io, metadata.missingmeta)
    #return t_pre
    t_pre[m] .= missing
    t_pre
end

column_loader(::Type{StringVector{T}}, io, metadata) where {T} = begin
    # uncompress
    args = Vector[]

    # assign the buffer once
    buffer = Vector{UInt8}(undef, maximum(x -> x[2], metadata.metadata))

    for (elm_type, compressed_bytes) in metadata.metadata
        readbytes!(io, buffer, compressed_bytes)
        push!(args, Blosc.decompress(elm_type, buffer))
    end
    metadata.type(args...)
end


# load a string column
"""
    metadata should consists of length, compressed byte size of string-lengths,
    string content lengths
"""
column_loader!(_, ::Type{String}, io, metadata) = begin
    buffer = Vector{UInt8}(undef, metadata.string_compressed_bytes)
    readbytes!(io, buffer, metadata.string_compressed_bytes)
    #return String(buffer)

    # read the string-lengths
    buffer2 = Vector{UInt8}(undef, metadata.string_len_bytes)
    readbytes!(io, buffer2, metadata.string_len_bytes)

    buffer3 = Vector{UInt8}(undef, metadata.rle_bytes)
    readbytes!(io, buffer3, metadata.rle_bytes)

    counts = Blosc.decompress(UInt64, buffer3)

    str_lens = Blosc.decompress(UInt32, buffer2)

    #return (String(buffer), str_lens, counts)

    lengths = inverse_rle(str_lens, counts)
    offsets = inverse_rle(vcat(0, cumsum(str_lens[1:end-1])), counts)

    #res = StringArray{String, 1}(buffer, vcat(1, cumsum(Blosc.decompress(UInt64, buffer3))[1:end-1]) .-1,  )
    res = StringArray{String,1}(buffer, offsets, lengths)
end

column_loader!(buffer, ::Type{Missing}, io, metadata) = nothing

"""
Saving a String
"""
compress_then_write(b::Vector{String}, io) = begin
    # TODO compare whether StringArray is better
    # return compress_then_write(StringArray(b), io)

    # write the string one by one
    # do a Run-length encoding (RLE)
    previous_b = b[1]
    cnt = 1
    lens = Int[]
    str_lens = Int[]
    for i = 2:length(b)
        if b[i] != previous_b
            push!(str_lens, write(io, previous_b))
            push!(lens, cnt)
            cnt = 0
            previous_b = b[i]
        end
        cnt += 1
    end

    # reach the end: two situation
    # 1) it's a new element, so write it
    # 2) it's an existing element. Also write it
    push!(str_lens, write(io, previous_b))
    push!(lens, cnt)

    @assert sum(lens) == length(b)

    str_lens_compressed = Blosc.compress(Vector{UInt32}(str_lens))
    str_lens_bytes = write(io, str_lens_compressed)

    lens_compressed = Blosc.compress(Vector{UInt64}(lens))
    rle_bytes = write(io, lens_compressed)

    # return metadata
    return (
        string_compressed_bytes = sum(str_lens),
        string_len_bytes = str_lens_bytes,
        rle_bytes = rle_bytes,
        rle_len = length(str_lens),
        type = String,
        len = max(sum(str_lens), str_lens_bytes, rle_bytes),
    )
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

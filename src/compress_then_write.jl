compress_then_write(b, io) = compress_then_write(eltype(b), b, io)

compress_then_write(::Type{Missing}, b, io) = (len = 0, type = Missing)

compress_then_write(T, b, io) = begin
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end

compress_then_write(::Type{Bool}, b, io) = begin
    b8 = UInt8.(b)
    bbc = Blosc.compress(b8)
    write(io, bbc)
    return (len = length(bbc), type = Bool)
end


compress_then_write(::Type{T}, b, io) where {T>:Missing} = begin
    S = nonmissingtype(eltype(b))
    b_S = coalesce.(b, some_elm(S))

    metadata = compress_then_write(S, b_S, io)

    b_m = ismissing.(b)
    metadata2 = compress_then_write(eltype(b_m), b_m, io)

    (
     Tmeta = metadata,
     missingmeta = metadata2,
     type = eltype(b),
     len = max(metadata.len, metadata2.len),
    )
end

compress_then_write(_, b::StringVector{T}, io) where {T} = compress_then_write(b, io)

compress_then_write(b::StringVector{T}, io) where {T} = begin
    fields = (getfield(b, n) for n in fieldnames(typeof(b)))
    (
     metadata = [(eltype(f), write(io, Blosc.compress(f))) for f in fields],
     type = typeof(b),
    )
end

# perform a RLE
compress_then_write(::Type{String}, b::Array{String}, io) = begin
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
            #push!(str_lens, sizeof(previous_b))
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
    #push!(str_lens, sizeof(previous_b))


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

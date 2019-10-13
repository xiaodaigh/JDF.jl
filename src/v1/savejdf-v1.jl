some_elm_v1(x) = zero(x)
some_elm_v1(::Type{Missing}) = missing
some_elm_v1(::Type{String}) = ""

compress_then_write_v1(b, io) = compress_then_write(eltype(b), b, io)

compress_then_write_v1(::Type{Missing}, b, io) = (len = 0, type = Missing)

compress_then_write_v1(T, b, io) = begin
    bbc = Blosc.compress(b)
    res = length(bbc)
    write(io, bbc)
    return (len = res, type = T)
end

"""
    save a DataFrames to the outdir
"""
savejdf_v1(outdir, df) =begin
    if VERSION < v"1.3.0-rc1"
        return ssavejdf(outdir, df)
    end

    pmetadatas = Any[missing for i = 1:length(names(df))]

    if !isdir(outdir)
        mkpath(outdir)
    end

    # use a bounded channel to limit
    c1 = Channel{Bool}(Threads.nthreads())
    atexit(()->close(c1))

    for (i, n) in enumerate(names(df))
        put!(c1, true)
        pmetadatas[i] = @spawn begin
            io = BufferedOutputStream(open(joinpath(outdir, string(n)) ,"w"))
            res = compress_then_write(Array(df[!, i]), io)
            close(io)
            res
        end
        take!(c1)
    end

    metadatas = fetch.(pmetadatas)

    fnl_metadata = (
        names = names(df),
        rows = size(df, 1),
        metadatas = metadatas
    )

    serialize(joinpath(outdir, "metadata.jls"), fnl_metadata)
    fnl_metadata
end

"""
    serially save a DataFrames to the outdir
"""
ssavejdf_v1(outdir, df::DataFrame) = begin
    pmetadatas = Any[missing for i = 1:length(names(df))]

    if !isdir(outdir)
        mkpath(outdir)
    end

    for i = 1:length(names(df))
        io = BufferedOutputStream(open(joinpath(outdir, string(names(df)[i])), "w"))
        pmetadatas[i] = compress_then_write(Array(df[!, i]), io)
        close(io)
    end


    fnl_metadata = (
        names = names(df),
        rows = size(df, 1),
        metadatas = pmetadatas
    )

    serialize(joinpath(outdir, "metadata.jls"), fnl_metadata)
    fnl_metadata
end

# figure out from metadata how much space is allocated
get_bytes_v1(metadata) = begin
    if metadata.type == String
        return max(
            metadata.string_compressed_bytes,
            metadata.string_len_bytes,
        )
    elseif metadata.type == Missing
        return 0
    elseif metadata.type >: Missing
        return max(
            get_bytes(metadata.Tmeta),
            get_bytes(metadata.missingmeta),
        )
    else
        return metadata.len
    end
end


hasfieldnames(::Type{T}) where {T} = fieldnames(T) >= 1

column_loader_v1(t::Type{T}, io, metadata) where {T} = begin
    buffer = Vector{UInt8}(undef, metadata.len)
    column_loader!(buffer, t, io, metadata)
end

# load bytes bytes from io decompress into type
column_loader!_v1(buffer, ::Type{T}, io, metadata) where {T} = begin
    readbytes!(io, buffer, metadata.len)
    return Blosc.decompress(T, buffer)
end

compress_then_write_v1(::Type{Bool}, b, io) = begin
    b8 = UInt8.(b)
    bbc = Blosc.compress(b8)
    write(io, bbc)
    return (len = length(bbc), type = Bool)
end

column_loader_v1(T::Type{Bool}, io, metadata) = begin
# Bool are saved as UInt8
    buffer = Vector{UInt8}(undef, metadata.len)
    readbytes!(io, buffer, metadata.len)
    Bool.(Blosc.decompress(UInt8, buffer))
end

column_loader!_v1(buffer, T::Type{Bool}, io, metadata) = begin
    # Bool are saved as UInt8
    read!(io, buffer)
    res = Blosc.decompress(UInt8, buffer)
    Bool.(res)
end

compress_then_write_v1(::Type{T}, b, io) where {T>:Missing} = begin
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

column_loader!_v1(buffer, ::Type{Union{Missing,T}}, io, metadata) where {T} = begin
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


compress_then_write_v1(_, b::StringVector{T}, io) where {T} = compress_then_write(b, io)

compress_then_write_v1(b::StringVector{T}, io) where {T} = begin
    fields = (getfield(b, n) for n in fieldnames(typeof(b)))
    (
     metadata = [(eltype(f), write(io, Blosc.compress(f))) for f in fields],
     type = typeof(b),
    )
end

column_loader_v1(::Type{StringVector{T}}, io, metadata) where {T} = begin
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

# perform a RLE
compress_then_write_v1(::Type{String}, b::Array{String}, io) = begin
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


# load a string column
"""
    metadata should consists of length, compressed byte size of string-lengths,
    string content lengths
"""
column_loader_v1!(_, ::Type{String}, io, metadata) = begin
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

column_loader_v1!(buffer, ::Type{Missing}, io, metadata) = nothing

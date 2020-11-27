using Tables

"""
    some_elm(::Type{T})

Some arbitrary element of type `T`
"""
some_elm(::Type{T}) where {T} = begin
    try
        return zero(T)
    catch
        try
            return T(0)
        catch
            try
                rand(T)
            catch
                try
                    Vector{T}(undef, 1)[1]
                catch
                    throw("the type $T is not supported by JDF.jl yet. Try to update JDF.jl. If it still doesn't work after update, please submit an issue at https://github.com/xiaodaigh/JDF.jl/issues")
                end
            end
        end
    end
end

"""
    JDF.save(outdir, table)

    JDF.save(table, outdir)

Save a `Tables.jl` compatitable table to the `outdir`. On Julia > v1.3, a multi-threaded version is
used.

The columns of the table can be of the following vector types columns are
supported

    * `isbits` types e.g. `Int*`, `UInt*`, `Float*`
    * `Bool`
    * `Strings`
    * `WeakRefStrings.StringVector`
    * `CategoricalArrays`
    * `Union{Missing, T}`` for `T` support above

"""
save(df, outdir::AbstractString; kwargs...) = save(outdir, df; kwargs...)

function save(outdir::AbstractString, df; verbose = false)
    @assert Tables.istable(df)

    if VERSION < v"1.3.0-rc1"
        return ssavejdf(outdir, df)
    end

    pmetadatas = Any[missing for i = 1:length(Tables.columnnames(df))]

    if !isdir(outdir)
        mkpath(outdir)
    end

    # use a bounded channel to limit the number simultaneous writes
    c1 = Channel{Bool}(Threads.nthreads())
    atexit(() -> close(c1))

    for (i, n) in enumerate(Tables.columnnames(df))
        if verbose
            println(n)
        end
        put!(c1, true)
        pmetadatas[i] = @spawn begin
            io = BufferedOutputStream(open(joinpath(outdir, string(n)), "w"))
            res = compress_then_write(Tables.getcolumn(df, n), io)
            close(io)
            res
        end
        take!(c1)
    end

    metadatas = fetch.(pmetadatas)

    fnl_metadata = (
        names = Tables.columnnames(df),
        rows = size(df, 1),
        metadatas = metadatas,
        version = v"0.2",
    )

    open(joinpath(outdir, "metadata.jls"), "w") do io
        serialize(io, fnl_metadata)
    end
    #fnl_metadata
    JDFFile(outdir)
end

"""
    Serially save a Tables.jl compatible table to the `outdir`
"""
function ssave(outdir::AbstractString, df)
    colnames = Tables.columnnames(df)
    pmetadatas = Any[missing for i = 1:length(colnames)]

    if !isdir(outdir)
        mkpath(outdir)
    end

    for i = 1:length(colnames)
        io = BufferedOutputStream(open(joinpath(outdir, string(colnames[i])), "w"))
        pmetadatas[i] = compress_then_write(Tables.getcolumn(df, colnames[i]), io)
        close(io)
    end


    fnl_metadata = (
        names = colnames,
        rows = size(df, 1),
        metadatas = pmetadatas,
        version = v"0.2",
    )

    open(joinpath(outdir, "metadata.jls"), "w") do io
        serialize(io, fnl_metadata)
    end
    JDFFile(outdir)
end

# figure out from metadata how much space is allocated
""" Get tthe number of bytes used by the file"""
get_bytes(metadata) = begin
    if metadata.type == String
        return max(metadata.string_compressed_bytes, metadata.string_len_bytes)
    elseif metadata.type == Missing
        return 0
    elseif metadata.type >: Missing
        return max(get_bytes(metadata.Tmeta), get_bytes(metadata.missingmeta))
    else
        return metadata.len
    end
end

hasfieldnames(::Type{T}) where {T} = fieldnames(T) >= 1

savejdf(args...; kwargs...) = save(args...; kwargs...)
ssavejdf(args...; kwargs...) = ssave(args...; kwargs...)

"""
    some_elm(::Type{T})

Some arbitrary element of type `T`
"""
some_elm(::Type{T}) where T = begin
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
    savejdf(outdir, dataframe)

    savejdf(dataframe, outdir)

Save a `DataFrame` to the `outdir`. On Julia > v1.3, a multithreaded version is
used.

The columns of the dataframe can be of the following vector types columns are
supported

    * `isbits` types e.g. `Int*`, `UInt*`, `Float*`
    * `Bool`
    * `Strings`
    * `WeakRefStrings.StringVector`
    * `CategoricalArrays`
    * `Union{Missing, T}`` for `T` support above

"""
savejdf(df::AbstractDataFrame, outdir::AbstractString) = savejdf(outdir, df)

savejdf(outdir, df::AbstractDataFrame; verbose = false) = begin
    if VERSION < v"1.3.0-rc1"
        return ssavejdf(outdir, df)
    end

    pmetadatas = Any[missing for i = 1:length(DataFrames.names(df))]

    if !isdir(outdir)
        mkpath(outdir)
    end

    # use a bounded channel to limit
    c1 = Channel{Bool}(Threads.nthreads())
    atexit(()->close(c1))

    for (i, n) in enumerate(DataFrames.names(df))
        if verbose
            println(n)
        end
        put!(c1, true)
        pmetadatas[i] = @spawn begin
            io = BufferedOutputStream(open(joinpath(outdir, string(n)) ,"w"))
            res = compress_then_write(df[!, i], io)
            close(io)
            res
        end
        take!(c1)
    end

    metadatas = fetch.(pmetadatas)

    fnl_metadata = (
        names = DataFrames.names(df),
        rows = DataFrames.size(df, 1),
        metadatas = metadatas,
        version = v"0.2"
    )

    open(joinpath(outdir, "metadata.jls"), "w") do io
        serialize(io, fnl_metadata)
    end
    fnl_metadata
end

"""
    serially save a DataFrames to the outdir
"""
ssavejdf(outdir, df::DataFrame) = begin
    pmetadatas = Any[missing for i = 1:length(DataFrames.names(df))]

    if !isdir(outdir)
        mkpath(outdir)
    end

    for i = 1:length(DataFrames.names(df))
        io = BufferedOutputStream(open(joinpath(outdir, string(DataFrames.names(df)[i])), "w"))
        pmetadatas[i] = compress_then_write(df[!, i], io)
        close(io)
    end


    fnl_metadata = (
        names = DataFrames.names(df),
        rows = DataFrames.size(df, 1),
        metadatas = pmetadatas,
        version = v"0.2"
    )

    open(joinpath(outdir, "metadata.jls"), "w") do io
        serialize(io, fnl_metadata)
    end
    fnl_metadata
end

# figure out from metadata how much space is allocated
get_bytes(metadata) = begin
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

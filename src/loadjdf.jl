
"""
    loadjdf(indir, verbose = true)

    loadjdf(indir, cols = Vector{Symbol}, verbose = true)

Load a `DataFrame` from JDF saved at `outdir`. On Julia > v1.3, a multithreaded
version is used.
"""
loadjdf(indir; cols = Symbol[], verbose = false) = begin
    # starting from DataFrames.jl 0.21 the colnames are strings
    cols = string.(cols)

    if VERSION < v"1.3.0"
        return sloadjdf(indir, cols = cols, verbose = verbose)
    end

    if verbose
        println("loading $indir in parallel")
    end

    metadatas = open(joinpath(indir, "metadata.jls")) do io
        deserialize(io)
    end

    if length(cols) == 0
        cols = string.(metadatas.names)
    else
        scmn = setdiff(cols, string.(metadatas.names))
        if length(scmn) > 0
            throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
        end
    end

    df = DataFrame()

    # get the maximum number of bytes needs to read
    # bytes_needed = maximum(get_bytes.(metadatas.metadatas))

    results = Vector{Any}(undef, length(cols))

    # rate limit channel
    c1 = Channel{Bool}(Threads.nthreads())
    atexit(() -> close(c1))

    i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
        name_str = string(name)
        if name_str in cols
            put!(c1, true)
            results[i] = @spawn begin
                io = BufferedInputStream(open(joinpath(indir, string(name)), "r"))
                new_result = column_loader(metadata.type, io, metadata)
                close(io)
                (name = name_str, task = new_result)
            end
            take!(c1)
            i += 1
        end
    end

    # run this serially
    #println(results)
    for result in results
        if verbose
            println("Extracting $(fetch(result).name)")
        end

        new_result = fetch(result).task
        colname = fetch(result).name
        if new_result == nothing
            df[!, Symbol(colname)] = Vector{Missing}(missing, metadatas.rows)
        else
            df[!, Symbol(colname)] = new_result
        end
    end
    df
end

loadjdf(jdf::JDFFile; args...) = loadjdf(path(jdf); args...)
sloadjdf(jdf::JDFFile; args...) = sloadjdf(path(jdf); args...)

# load the data from file with a schema
sloadjdf(indir; cols = Symbol[], verbose = false) = begin
    metadatas = open(joinpath(indir, "metadata.jls")) do io
        deserialize(io)
    end

    if length(cols) == 0
        cols = metadatas.names
    else
        scmn = setdiff(cols, metadatas.names)
        if length(scmn) > 0
            throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
        end
    end

    df = DataFrame()

    # get the maximum number of bytes needs to read
    # bytes_needed = maximum(get_bytes.(metadatas.metadatas))

    # rate limit channel
    #results = Vector{Any}(undef, length(metadatas.names))
    results = Vector{Any}(undef, length(cols))

    i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
        if name in cols
            if verbose
                println("Loading $name")
            end
            results[i] = begin
                io = BufferedInputStream(open(joinpath(indir, string(name)), "r"))
                new_result = column_loader(metadata.type, io, metadata)
                close(io)
                (name = name, task = new_result)
            end
            i += 1
        end
    end

    # run this serially
    for result in results
        if verbose
            println("Extracting $(result.name)")
            println(result.task)
        end

        new_result = result.task
        colname = result.name
        if new_result == nothing
            df[!, colname] = Vector{Missing}(missing, metadatas.rows)
        else
            df[!, colname] = new_result
        end
    end
    df
end

load(args...; kwargs...) = loadjdf(args...; kwargs...)
sload(args...; kwargs...) = sloadjdf(args...; kwargs...)

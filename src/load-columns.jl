"""
Load the columns of the JDF as vectors
"""

load_columns(jdf::JDFFile; args...) = load_columns(path(jdf); args...)

function load_columns(indir; cols = Symbol[], verbose = false)
    # starting from DataFrames.jl 0.21 the colnames are strings
    cols = string.(cols)

    if VERSION < v"1.3.0"
        return sload_columns(indir, cols = cols, verbose = verbose)
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

    # run the collection of results this serially
    result_vectors = Vector{Any}(undef, length(cols))
    for (i, result) in enumerate(results)
        if verbose
            println("Extracting $(fetch(result).name)")
        end

        new_result = fetch(result).task
        colname = fetch(result).name
        if new_result == nothing
            result_vectors[i] = Vector{Missing}(missing, metadatas.rows)
        else
            result_vectors[i] = new_result
        end
    end

    return result_vectors
end

function sload_columns(indir; cols=Symbol[], verbose=false)
    metadatas = open(joinpath(indir, "metadata.jls")) do io
        deserialize(io)
    end

    if length(cols) == 0
        cols = metadatas.names
    else
        scmn = setdiff(cols, string.(metadatas.names))
        if length(scmn) > 0
            throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn[1:min(8, length(scmn))])) are not available, please ensure you have spelt them correctly")
        end
    end

    # get the maximum number of bytes needs to read
    # bytes_needed = maximum(get_bytes.(metadatas.metadatas))

    # rate limit channel
    #results = Vector{Any}(undef, length(metadatas.names))
    results = Vector{Any}(undef, length(cols))

    i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
        if string(name) in cols
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

    result_vectors = Vector{Any}(undef, length(results))

    # run this serially
    for (i, result) in enumerate(results)
        if verbose
            println("Extracting $(result.name)")
            println(result.task)
        end

        new_result = result.task
        colname = result.name
        if new_result === nothing
           result_vectors[i] = Vector{Missing}(missing, metadatas.rows)
        else
            result_vectors[i] = new_result
        end
    end

    return result_vectors
end

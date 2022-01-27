"""
Load the columns of the JDF as vectors
"""

load_columns(jdf::JDFFile; args...) = load_columns(path(jdf); args...)

function load_columns(indir; cols = Symbol[], verbose = false)
    # starting from DataFrames.jl 0.21 the colnames are strings
    cols = string.(cols)

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

    results = Vector{Any}(undef, length(cols))
    names = Vector{String}(undef, length(cols))

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
        names[i] = colname
        if new_result === nothing
            result_vectors[i] = Vector{Missing}(missing, metadatas.rows)
        else
            result_vectors[i] = new_result
        end
    end

    return names, result_vectors
end

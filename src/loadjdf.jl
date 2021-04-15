"""
    JDF.load(indir, verbose = true)

    JDF.load(indir, cols = Vector{Symbol}, verbose = true)

Load a `Tables.jl` table from JDF saved at `outdir`. On Julia > v1.3, a multithreaded
version is used.
"""
load(indir; cols = Symbol[], verbose = false) = begin
    # starting from DataFrames.jl 0.21 the colnames are strings
    cols = string.(cols)
    metadatas = jdfmetadata(indir)

    # TODO simplify this this is duplicated in load_columns
    if length(cols) == 0
        cols = string.(metadatas.names)
    else
        scmn = setdiff(cols, string.(metadatas.names))
        if length(scmn) > 0
            throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
        end
    end

    result_vectors = load_columns(indir; cols = cols, verbose = verbose)

    Table(NamedTuple{Tuple(Symbol.(cols))}(result_vectors))
end

load(jdf::JDFFile; args...) = load(path(jdf); args...)
sload(jdf::JDFFile; args...) = sload(path(jdf); args...)

# load the data from file with a schema
function sload(indir; cols = Symbol[], verbose = false)
    cols = string.(cols)
    metadatas = jdfmetadata(indir)

    # TODO simplify this
    if length(cols) == 0
        cols = string.(metadatas.names)
    else
        scmn = setdiff(cols, string.(metadatas.names))
        if length(scmn) > 0
            throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
        end
    end

    result_vectors = sload_columns(indir; cols = cols, verbose = verbose)
    Table(NamedTuple{Tuple(Symbol.(cols))}(result_vectors))
end

loadjdf(args...; kwargs...) = load(args...; kwargs...)
sloadjdf(args...; kwargs...) = sload(args...; kwargs...)

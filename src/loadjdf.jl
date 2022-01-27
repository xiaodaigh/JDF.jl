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

    cols_in_loaded_order, result_vectors = load_columns(indir; cols = cols, verbose = verbose)

    Table(NamedTuple{Tuple(Symbol.(cols_in_loaded_order))}(result_vectors))
end

load(jdf::JDFFile; args...) = load(path(jdf); args...)


loadjdf(args...; kwargs...) = load(args...; kwargs...)


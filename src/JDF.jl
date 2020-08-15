__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using Missings: Missings
using BufferedStreams
#using RLEVectors
using WeakRefStrings, TimeZones

using StatsBase: rle, inverse_rle, countmap, sample

import Base: show, getindex, setindex!, eltype, names

using Base: size#, @v_str, >=, include, VERSION

using Serialization: serialize, deserialize

import DataFrames: nrow, ncol

if VERSION >= v"1.3.0"
    import Base.Threads: @spawn
else
    macro spawn(_)
        println("JDF: parallel save/load do not work in < Julia 1.3")
    end
end

function __init__()
    if VERSION >= v"1.3.0"
        Blosc.set_num_threads(Threads.nthreads())
    else
        Blosc.set_num_threads(isdefined(Sys, :CPU_CORES) ? Sys.CPU_CORES : Sys.CPU_THREADS)
    end
end

export savejdf, loadjdf, ssavejdf, sloadjdf#, save, load
export column_loader, column_loader!
export type_compress!, type_compress
export compress_then_write
export JDFFile, @jdf_str, jdfmetadata, metadata, nrow, ncol, size, names
export IsBitsType, eachcol, some_elm, getindex, istable


include("JDFFile.jl")
include("type-writer-loader/Bool.jl")
include("type-writer-loader/Char.jl")
include("type-writer-loader/categorical-arrays.jl")
include("type-writer-loader/pooled-arrays.jl")
include("type-writer-loader/Missing.jl")
include("type-writer-loader/Nothing.jl")
include("type-writer-loader/String.jl")
include("type-writer-loader/StringArray.jl")
include("type-writer-loader/Symbol.jl")
include("type-writer-loader/ZonedDateTime.jl")
include("type-writer-loader/substring.jl")

include("column_loader.jl")
include("compress_then_write.jl")

include("loadjdf.jl")
include("savejdf.jl")
include("type_compress.jl")

include("metadata.jl")
include("eachcol.jl")
include("dataframe-syntax.jl")
include("Tables.jl")

# Blosc.set_num_threads(6)



end # module

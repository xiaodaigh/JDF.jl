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

using Base:size#, @v_str, >=, include, VERSION

using Serialization: serialize, deserialize
using CSV

import DataFrames: nrow, ncol

if VERSION >= v"1.3.0-rc1"
    import Base.Threads: @spawn
else
    macro spawn(_)
        println("JDF: parallel save/load do not work in < Julia 1.3")
    end
end


function __init__()
    if VERSION >= v"1.3.0-rc1"
        Blosc.set_num_threads(Threads.nthreads())
    else
        Blosc.set_num_threads(isdefined(Sys, :CPU_CORES) ? Sys.CPU_CORES : Sys.CPU_THREADS)
    end
end

export savejdf, loadjdf, ssavejdf, sloadjdf
export column_loader, column_loader!
export type_compress!, type_compress
export compress_then_write
export JDFFile, @jdf_str, jdfmetadata, metadata, nrow, ncol, size, names
export IsBitsType, eachcol, some_elm
"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you can apply `nrow`, `ncol`, `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
nrow(jdf"plsdel.jdf") # 3
ncol(jdf"plsdel.jdf") # 2
size(jdf"plsdel.jdf") # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

# clean up
rm("plsdel.jdf", force = true, recursive = true)
"""
struct JDFFile{T <: AbstractString}
    path::T
end

"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you can apply `nrow`, `ncol`, `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
nrow(jdf"plsdel.jdf") # 3
ncol(jdf"plsdel.jdf") # 2
size(jdf"plsdel.jdf") # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

# clean up
rm("plsdel.jdf", force = true, recursive = true)
"""
macro jdf_str(path)
    return :(JDFFile($path))
end

include("type-writer-loader/Bool.jl")
include("type-writer-loader/Char.jl")
include("type-writer-loader/categorical-arrays.jl")
include("type-writer-loader/Missing.jl")
include("type-writer-loader/Nothing.jl")
include("type-writer-loader/String.jl")
include("type-writer-loader/StringArray.jl")
include("type-writer-loader/Symbol.jl")
include("type-writer-loader/ZonedDateTime.jl")

include("column_loader.jl")
include("compress_then_write.jl")

include("loadjdf.jl")
include("savejdf.jl")
include("type_compress.jl")

include("metadata.jl")
include("eachcol.jl")

# Blosc.set_num_threads(6)



end # module

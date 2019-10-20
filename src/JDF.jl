__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using Missings: Missings
using BufferedStreams
#using RLEVectors
using WeakRefStrings

using StatsBase: rle, inverse_rle, countmap, sample

import Base: show, getindex, setindex!, eltype, names

using Base:size#, @v_str, >=, include, VERSION

using Serialization: serialize, deserialize

import DataFrames: nrow, ncol

if VERSION >= v"1.3.0-rc1"
    import Base.Threads: @spawn
else
    macro spawn(_)
        println("JDF: parallel save/load do not work in < Julia 1.3")
    end
end

"Define a JDF file"
struct JDFFile{T <: AbstractString}
    path::T
end

"Define a JDF file"
macro jdf_str(path)
    return :(JDFFile($path))
end

export savejdf, loadjdf, ssavejdf, sloadjdf
export column_loader, column_loader!
export type_compress!, type_compress
export compress_then_write
export JDFFile, @jdf_str, jdfmetadata, metadata, nrow, ncol, size, names

include("type-writer-loader/Bool.jl")
include("type-writer-loader/categorical-arrays.jl")
include("type-writer-loader/Missing.jl")
include("type-writer-loader/String.jl")
include("type-writer-loader/StringArray.jl")

include("column_loader.jl")
include("compress_then_write.jl")

include("loadjdf.jl")
include("savejdf.jl")
include("type_compress.jl")

include("metadata.jl")

# Blosc.set_num_threads(6)



end # module

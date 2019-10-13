__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using Missings: Missings
using StatsBase: rle, inverse_rle, sample
using BufferedStreams
#using RLEVectors
using WeakRefStrings
using Blosc


using StatsBase: rle, inverse_rle, countmap

import Base: size, show, getindex, setindex!, eltype

if VERSION >= v"1.3.0-rc1"
    import Base.Threads: @spawn
else
    macro spawn(x)
        println("parallel version do not work in < Julia 1.3")
    end
end

# if VERSION >= v"1.1"
using Serialization: serialize, deserialize
# else
#     using Compat.Serialization: serialize, deserialize
# end


export savejdf, loadjdf, nonmissingtype, gf, iow, ior, compress_then_write
export column_loader!, gf2, ssavejdf, type_compress!, type_compress, sloadjdf
export column_loader

include("categorical-arrays.jl")
include("column_loader.jl")
include("compress_then_write.jl")
include("loadjdf.jl")
include("savejdf.jl")
include("type_compress.jl")


# Blosc.set_num_threads(6)

end # module

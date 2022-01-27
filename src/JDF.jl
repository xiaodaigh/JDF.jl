module JDF

using Blosc: Blosc
using Missings: Missings
using BufferedStreams
#using RLEVectors
using WeakRefStrings, TimeZones

using StatsBase: rle, inverse_rle, countmap, sample

import Base: show, getindex, setindex!, eltype, names

using Base: size

using Serialization: serialize, deserialize


import Base.Threads: @spawn

function __init__()
    Blosc.set_num_threads(Threads.nthreads())
end

export savejdf, loadjdf
export column_loader, column_loader!
export type_compress!, type_compress
export compress_then_write
export JDFFile, @jdf_str, jdfmetadata, metadata, size, names
export IsBitsType, eachcol, some_elm, getindex, istable


include("JDFFile.jl")
include("type-writer-loader/Bool.jl")
include("type-writer-loader/Char.jl")
include("type-writer-loader/DateTime.jl")
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

include("load-columns.jl")
include("loadjdf.jl")
include("savejdf.jl")
include("type_compress.jl")

include("metadata.jl")
include("eachcol.jl")
include("Tables.jl")

end # module

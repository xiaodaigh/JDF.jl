# using Revise
using Test
using JDF, WeakRefStrings, DataFrames

@testset "JDF.jl WeakRefStrings.StringArrays" begin
    a = StringVector(["a", "b", "a", missing, "c"])
    io = open("test.io", "w")
    metadata = compress_then_write(a, io)
    close(io)

    io = open("test.io", "r")
    aa = column_loader(StringVector{String}, io, metadata)
    close(io)

    df = DataFrame(a = a)
    savejdf("pls_del.jdf", df)
    loadjdf("pls_del.jdf")

    rm("pls_del.jdf", force=true, recursive=true)
end

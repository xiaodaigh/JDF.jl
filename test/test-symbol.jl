# using Revise
using Test
using JDF, DataFrames

@testset "JDF.jl Symbol" begin
    ac = [:a, :b, :a, :c, :d]
    metadata_ac = open("io.jdf", "w") do io
        compress_then_write(ac, io)
    end


    a = [:a, :b, :a, missing, :c]
    metadata = open("io.jdf", "w") do io
        compress_then_write(a, io)
    end

    aa = open("io.jdf", "r") do io
        column_loader(Union{Missing, Symbol}, io, metadata)
    end

    df = DataFrame(a = a, ac = ac)
    savejdf("pls_del.jdf", df)
    df2 = loadjdf("pls_del.jdf")

    @test size(df) == size(df2)
    @test all(isequal.(df.a, df2.a))
    @test all(isequal.(df.ac, df2.ac))

    rm("io.jdf", force=true)
    rm("pls_del.jdf", force=true, recursive=true)
end

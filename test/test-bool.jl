# using Revise
using Test
using JDF, DataFrames
using Missings: missing

@testset "JDF.jl missing bool" begin
    ac = [missing, true, false, missing, true, true, missing]

    df = DataFrame(ac=ac)
    JDF.save("pls_del.jdf", df)
    df2 = JDF.load("pls_del.jdf") |> DataFrame

    @test size(df) == size(df2)
    @test all(isequal.(df.ac, df2.ac))

    rm("pls_del.jdf", force=true, recursive=true)
end

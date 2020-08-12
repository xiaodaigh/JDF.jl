using Test
using JDF
using DataFrames: DataFrame
using PooledArrays
using Missings: allowmissing

@testset "JDF.jl pooledarrays arrays" begin
    df = DataFrame([rand(1:10, 100) for i =1:3])
    df[!, :x3] = df[:, :x1] |> allowmissing
    df[!, :x4] = string.(df[!, :x2]) |> allowmissing
    df[50, :x3] = missing
    df[66, :x4] = missing

    df[!, :x1] = PooledArray(df[!, :x1])
    df[!, :x2] = PooledArray(string.(df[!, :x2]))
    df[!, :x3] = PooledArray(df[!, :x3])
    df[!, :x4] = PooledArray(df[!, :x4])

    JDF.save(df, "a3pooled.jdf")
    df2 = JDF.load("a3pooled.jdf")
    @test size(df2, 2) == 4
    @test size(df2, 1) == 100
    @time df2[!, :x1] isa PooledVector{Int}
    @time df2[!, :x2] isa PooledVector{String}
    @time df2[!, :x3] isa PooledVector{Union{Missing, Int}}
    @time df2[!, :x4] isa PooledVector{Union{Missing, String}}

    for n in names(df)
        @test isequal(df2[n], df[n])
    end

    rm("a3pooled.jdf", force=true, recursive=true)
end
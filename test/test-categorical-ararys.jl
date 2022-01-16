using Test
using JDF
using RDatasets
using DataFrames: DataFrame
using CategoricalArrays
using Tables

@testset "JDF.jl categorical arrays" begin
    df = DataFrame([collect(1:100) for i = 1:3], :auto)
    df[!, :x1] = categorical(df[!, :x1])
    df[!, :x2] = categorical(string.(df[!, :x2]))
    JDF.save(df, "a3cate.jdf")
    df_loaded_back = JDF.load("a3cate.jdf", cols = [:x2, :x1])

    df2 = DataFrame(df_loaded_back; copycols = true)
    @test size(df2, 2) == 2
    @test size(df2, 1) == 100
    @time df2[!, :x1] isa CategoricalVector{Int}
    @time df2[!, :x2] isa CategoricalVector{String}

    rm("a3cate.jdf", force = true, recursive = true)
end

@testset "Guard against Github #27" begin
    iris = dataset("datasets", "iris")
    JDF.save(iris, "iris.jdf")
    JDF.load("iris.jdf")

    rm("iris.jdf", force = true, recursive = true)
end

@testset "CategoricalArray{Union{Missing, String}}" begin
    # Guard against github 73
    df2 = DataFrame(sex = categorical(["Male", missing, "Female"]))
    JDF.save("df2.jdf", df2)

    b = JDF.load("df2.jdf") |> DataFrame

    @test any(ismissing, b.sex)

    rm("df2.jdf", force=true, recursive=true)
end
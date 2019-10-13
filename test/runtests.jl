using JDF
using Test
using DataFrames

include("test-categorical-ararys.jl")

@testset "JDF.jl parallel" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    savejdf("a.jdf", df)

    @test size(loadjdf("a.jdf"), 2) == 3000
    @test size(loadjdf("a.jdf"), 1) == 100

    # clean up
    rm("a.jdf", force=true, recursive=true)
end

@testset "JDF.jl serial" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    ssavejdf("a2.jdf", df)

    @test size(sloadjdf("a2.jdf"), 2) == 3000
    @test size(sloadjdf("a2.jdf"), 1) == 100

    rm("a2.jdf", force=true, recursive=true)
end

@testset "JDF.jl categorical" begin
    df = DataFrame([collect(1:100) for i =1:3])
    df[!, :x1] = categorical(df[!, :x1])
    df[!, :x2] = categorical(string.(df[!, :x2]))
    ssavejdf("a2cate.jdf", df)

    @test size(sloadjdf("a2cate.jdf"), 2) == 3
    @test size(sloadjdf("a2cate.jdf"), 1) == 100
    @time df[!, :x1] isa CategoricalVector{Int}
    @time df[!, :x2] isa CategoricalVector{String}

    rm("a2cate.jdf", force=true, recursive=true)
end

using Test
# using Revise
using JDF

@testset "JDF.jl categorical arrays" begin
    df = DataFrame([collect(1:100) for i =1:3])
    df[!, :x1] = categorical(df[!, :x1])
    df[!, :x2] = categorical(string.(df[!, :x2]))
    savejdf("a3cate.jdf", df)

    df2 = loadjdf("a3cate.jdf", cols=[:x2, :x1])
    @test size(df2, 2) == 2
    @test size(df2, 1) == 100
    @time df2[!, :x1] isa CategoricalVector{Int}
    @time df2[!, :x2] isa CategoricalVector{String}

    rm("a3cate.jdf", force=true, recursive=true)
end

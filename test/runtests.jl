using JDF
using Test
using DataFrames

@testset "JDF.jl" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    savejdf("a.jdf", df)

    @test size(loadjdf("a.jdf"), 2) == 3000

    # clean up
    rm("a.jdf", force=true, recursive=true)
end

using DataFrames, Random, WeakRefStrings
using JDF

@testset "JDF.jl nrow" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    df[!, :int_missing] =  rand([rand(rand([UInt, Int, Float64, Float32, Bool])), missing], nrow(df))
    df[!, :missing] .= missing
    df[!, :strs] = [randstring(8) for i in 1:nrow(df)]
    df[!, :stringarray] = StringVector([randstring(8) for i in 1:nrow(df)])

    df[!, :strs_missing] = [rand([missing, randstring(8)]) for i in 1:nrow(df)]
    df[!, :stringarray_missing] = StringVector([rand([missing, randstring(8)]) for i in 1:nrow(df)])

    savejdf("a.jdf", df)

    dfjdf = jdf"a.jdf"

    @test ncol(jdf"a.jdf") == 3006
    @test nrow(jdf"a.jdf") == 100
    @test size(jdf"a.jdf") == (100, 3006)
    @test size(jdf"a.jdf", 1) == 100
    @test size(jdf"a.jdf", 2) == 3006

    # clean up
    rm("a.jdf", force=true, recursive=true)
end

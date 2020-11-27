using Test
using DataFrames
using Random: randstring
using WeakRefStrings: StringVector

@testset "JDF.jl eachcol" begin
    df = DataFrame([collect(1:100) for i = 1:3000])
    df[!, :int_missing] =
        rand([rand(rand([UInt, Int, Float64, Float32, Bool])), missing], size(df, 1))

    df[!, :missing] .= missing
    df[!, :strs] = [randstring(8) for i = 1:size(df, 1)]
    df[!, :stringarray] = StringVector([randstring(8) for i = 1:size(df, 1)])

    df[!, :strs_missing] = [rand([missing, randstring(8)]) for i = 1:size(df, 1)]
    df[!, :stringarray_missing] =
        StringVector([rand([missing, randstring(8)]) for i = 1:size(df, 1)])
    df[!, :symbol_missing] = [rand([missing, Symbol(randstring(8))]) for i = 1:size(df, 1)]
    df[!, :char] = getindex.(df[!, :strs], 1)
    df[!, :char_missing] = allowmissing(df[!, :char])
    df[rand(1:size(df, 1), 10), :char_missing] .= missing

    JDF.save("a.jdf", df)

    df2 = jdf"a.jdf"

    @test size(df2, 2) == 3009
    @test size(df2, 1) == 100

    df3 = [a for a in eachcol(df2)]

    df4 = DataFrame(df3)

    @test size(df4) == size(df)
    @test all([isequal(df4[!, n], df[!, n]) for n = 1:size(df4, 2)])

    # clean up
    rm("a.jdf", force = true, recursive = true)
end

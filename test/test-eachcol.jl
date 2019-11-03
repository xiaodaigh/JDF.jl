@testset "JDF.jl eachcol" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    df[!, :int_missing] =
        rand(
            [rand(rand([UInt, Int, Float64, Float32, Bool])), missing],
            nrow(df)
        )

    df[!, :missing] .= missing
    df[!, :strs] = [randstring(8) for i in 1:nrow(df)]
    df[!, :stringarray] = StringVector([randstring(8) for i in 1:nrow(df)])

    df[!, :strs_missing] = [rand([missing, randstring(8)]) for i in 1:nrow(df)]
    df[!, :stringarray_missing] = StringVector([rand([missing, randstring(8)]) for i in 1:nrow(df)])
    df[!, :symbol_missing] = [rand([missing, Symbol(randstring(8))]) for i in 1:nrow(df)]
    df[!, :char] = getindex.(df[!, :strs], 1)
    df[!, :char_missing] = allowmissing(df[!, :char])
    df[rand(1:nrow(df), 10), :char_missing] .= missing

    savejdf("a.jdf", df)

    df2 = jdf"a.jdf"

    @test ncol(df2) == 3009
    @test nrow(df2) == 100    

    df3 = [a for a in eachcol(df2)]

    df4 = DataFrame(df3)

    @test size(df4) == size(df)
    @test all([isequal(df4[!, n], df[!, n]) for n in 1:ncol(df4)])

    # clean up
    rm("a.jdf", force=true, recursive=true)
end

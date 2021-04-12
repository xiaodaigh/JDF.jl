@testset "JDF.jl jdf_str" begin
    df = DataFrame([collect(1:100) for i = 1:3000], :auto)
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

    df2 = DataFrame(JDF.load(jdf"a.jdf"), copycols=false)

    @test size(df2, 2) == 3009
    @test size(df2, 1) == 100

    @test all(all(isequal(df[!, n], df2[!, n])) for n in names(df))

    df3 = DataFrame(JDF.load(jdf"a.jdf", cols = [:missing, :strs]), copycols=false)

    @test size(df3, 2) == 2
    @test size(df3, 1) == 100
    # clean up
    rm("a.jdf", force = true, recursive = true)
end

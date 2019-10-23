using JDF
using Test
using DataFrames
using Random:randstring
using WeakRefStrings

include("test-categorical-ararys.jl")
include("test-stringarray.jl")
include("test-symbol.jl")

@testset "JDF.jl parallel" begin
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

savejdf("a.jdf", df)

df2 = loadjdf("a.jdf")
@test ncol(df2) == 3007
@test nrow(df2) == 100

@test all(all(isequal(df[!,n], df2[!, n])) for n in names(df))

# clean up
rm("a.jdf", force=true, recursive=true)
end

@testset "JDF.jl serial" begin
    df = DataFrame([collect(1:100) for i =1:3000])
    df[!, :int_missing] =  rand([rand(rand([UInt, Int, Float64, Float32, Bool])), missing], nrow(df))
    df[!, :missing] .= missing
    df[!, :strs] = [randstring(8) for i in 1:nrow(df)]
    df[!, :stringarray] = StringVector([randstring(8) for i in 1:nrow(df)])

    df[!, :strs_missing] = [rand([missing, randstring(8)]) for i in 1:nrow(df)]
    df[!, :stringarray_missing] = StringVector([rand([missing, randstring(8)]) for i in 1:nrow(df)])
    df[!, :symbol_missing] = [rand([missing, Symbol(randstring(8))]) for i in 1:nrow(df)]

    ssavejdf("a.jdf", df)

    df2 = sloadjdf("a.jdf")
    @test ncol(df2) == 3007
    @test nrow(df2) == 100

    @test all(all(isequal(df[!,n], df2[!, n])) for n in names(df))

    # clean up
    rm("a.jdf", force=true, recursive=true)
end

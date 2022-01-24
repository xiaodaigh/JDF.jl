# Based on the Julia implementation
# InlineStringN are all `isbitstype` so they work with JDF automatically
# However for other languages, we may still need to explicitly support them and conver them
# to string is not available in those languages
using JDF
using InlineStrings, DataFrames, CSV

using Random:randstring

@testset "Test InlineStrings get loaded and saved properly" begin
    a = DataFrames.DataFrame(a = [randstring(254) |> String255 for i in 1:100])

    path = tempdir()

    CSV.write(joinpath(path, "tmp.csv"), a)

    a1 = CSV.read(joinpath(path, "tmp.csv"), DataFrame)

    a1.a = a1.a .|> String255

    JDF.save(joinpath(path, "tmp.jdf"), a1)

    a2 = JDF.load(joinpath(path, "tmp.jdf")) |> DataFrame

    @test eltype(a2.a) == String255

    # clean up
    rm("tmp.csv"; force=true)
    rm("tmp.jdf"; force=true, recursive=true)
end

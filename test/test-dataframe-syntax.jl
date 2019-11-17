using Test
# using Revise
using JDF

@testset "JDFFile" begin
    df = DataFrame([collect(1:100) for i =1:3])
    df[!, :x1] = categorical(df[!, :x1])
    df[!, :x2] = categorical(string.(df[!, :x2]))
    savejdf("a3dfsyntax.jdf", df)

    jdffile = JDFFile("a3dfsyntax.jdf")
    jdffile[!, :x1]
    jdffile[!, [:x1, :x2]]

    jdffile[:, :x1]
    jdffile[:, [:x1, :x2]]

    jdffile[[1, 100], :x1]
    jdffile[1:100, :x1]

    jdffile[rand(Bool, 100), :x1]

    rm("a3dfsyntax.jdf", force=true, recursive=true)
end

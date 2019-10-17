using Revise
using DataFrames, CSV, JDF
using WeakRefStrings

a[!, :stringarr] = StringArray(rand(["a", "a", "b"], size(a,1)))
a[!, :cate] = categorical(a[!, :stringarr])

@time a = CSV.read("c:/data/feature_matrix_cleaned.csv");

@time savejdf("c:/data/feature_matrix_cleaned.csv.jdf", a)

a = nothing
@time a = loadjdf("c:/data/feature_matrix_cleaned.csv.jdf")

type_compress!(a, compress_float=true)
@time savejdf("c:/data/feature_matrix_cleaned.csv.compressed.jdf", a)

using BenchmarkTools
@benchmark a = loadjdf("c:/data/feature_matrix_cleaned.csv.jdf")

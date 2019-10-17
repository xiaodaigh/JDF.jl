using Revise
using DataFrames, CSV, JDF
using WeakRefStrings

a[!, :stringarr] = StringArray(rand(["a", "a", "b"], size(a,1)))
a[!, :cate] = categorical(a[!, :stringarr])

@time a = CSV.read("c:/data/feature_matrix_cleaned.csv");

@time savejdf("c:/data/feature_matrix_cleaned.csv.jdf", a)

a = nothing
@time a = loadjdf("c:/data/feature_matrix_cleaned.csv.jdf")



a[!, names(a)[2]]

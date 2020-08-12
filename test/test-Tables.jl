using Test, RDatasets
using JDF, Tables

iris = dataset("datasets", "iris")
ok = savejdf(iris, "plsdel.jdf")

Tables.columns(ok)
Tables.schema(ok)
propertynames(ok)
getproperty(ok, :Species)


rm("plsdel.jdf", force = true, recursive = true)

using Revise
using JDF
using CSV, DataFrames, Feather

@time a = CSV.read("c:/data/a.feather");

@time metadatas = savejdf(a, "c:/data/a.jdf");
#a = nothing
@time a2 = loadjdf("c:/data/a.jdf", metadatas);

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

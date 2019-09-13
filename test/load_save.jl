using Revise
using JDF
using CSV, DataFrames
using Serialization:serialize,deserialize

@time a = gf()

@time metadatas = savejdf(a, "c:/data/a_pre_compress.jdf");

before = Base.summarysize(a)

@time type_compress!(a; verbose = true)

after = Base.summarysize(a)

before/after
2+2

@time metadatas = savejdf(a, "c:/data/a.jdf");
serialize("c:/data/a.jls", metadatas)

using Revise
using DataFrames, JDF
metadatas = deserialize("c:/data/a.jls")
@time a2 = loadjdf("c:/data/a.jdf", metadatas);

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

Base.summarysize(a2)

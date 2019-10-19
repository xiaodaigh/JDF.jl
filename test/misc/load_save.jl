using Revise
using JDF
using CSV, DataFrames
using Serialization:serialize,deserialize

@time a = gf()

@time metadatas = savejdf("c:/data/a_pre_compress.jdf", a);

# before = Base.summarysize(a)
#
@time type_compress!(a; verbose = true)
#
# after = Base.summarysize(a)
#
# before/after
# 2+2

@time savejdf("c:/data/a.jdf", a);

using Revise
using DataFrames, JDF
@time a2 = loadjdf("c:/data/a.jdf");

@time a2 = sloadjdf("c:/data/a.jdf");

type_compress!(a2)

@time savejdf("c:/data/a_p.jdf", a2)
@time ssavejdf("c:/data/a_p.jdf", a2)

#a2 = ploadjdf("c:/data/a_p.jdf")

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

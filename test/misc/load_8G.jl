using Revise

using JDF, CSV, DataFrames, Blosc, Base.GC, BenchmarkTools, Serialization

# use 12 threads
Blosc.set_num_threads(6)
# GC.gc()
@time a = CSV.read(
    "C:/data/Performance_All/Performance_2003Q1.txt",
    delim = '|',
    header = false
);
GC.gc()

t = time()
@time metadatas = psavejdf(a, "c:/data/large8.dir.jdf");
time() - t
GC.gc()

GC.gc()
@time metadatas = savejdf(a, "c:/data/large8.jdf");
GC.gc()
serialize("c:/data/large8.meta", metadatas)


using Revise, JDF, DataFrames, Serialization
metadatas = deserialize("c:/data/large8.meta")
@time a2 = loadjdf("c:/data/large8.jdf", metadatas);
# before=Base.summarysize(a2)

@time type_compress!(a2, verbose = true)

# after=Base.summarysize(a2)
#
# before/after

GC.gc()
@time metadatas = savejdf(a2, "c:/data/large8c.jdf");
GC.gc()
serialize("c:/data/large8c.meta", metadatas)

using Serialization, JDF
metadatas = deserialize("c:/data/large8c.meta")
@time a2 = loadjdf("c:/data/large8c.jdf", metadatas);

GC.gc()

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

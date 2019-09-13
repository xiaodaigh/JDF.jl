using Revise

using JDF, CSV, DataFrames, Blosc, Base.GC, BenchmarkTools, Serialization

# use 12 threads
Blosc.set_num_threads(6)
# GC.gc()
@time a = CSV.read(
    "C:/data/Performance_All/Performance_2010Q3.txt",
    delim = '|',
    header = false
);
GC.gc()

@time metadatas = savejdf("c:/data/large.jdf", a);

using Revise, JDF, DataFrames, Serialization
@time a2 = loadjdf("c:/data/large.jdf");
GC.gc()

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

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
@time a2 = loadjdf("c:/data/large.jdf"; verbose);
GC.gc()

@time type_compress!(a2);


io = iow()
compress_then_write(a2.Column2, io)
close(io)

@time psavejdf("c:/data/largec.jdf", a2);

using Revise, JDF, DataFrames, Serialization
@time a2 = ploadjdf("c:/data/largec.jdf");
GC.gc()

@time type_compress!(a2, verbose = true)

@time psavejdf("c:/data/largec2.jdf", a2);
@time a2 = loadjdf("c:/data/largec.jdf");

using Revise, JDF, DataFrames, Serialization
@time a2 = ploadjdf("c:/data/largec.jdf");
GC.gc()



all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

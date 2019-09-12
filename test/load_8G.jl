using Revise

using JDF

using CSV, DataFrames, Blosc, JLSO, Base.GC

# use 12 threads
Blosc.set_num_threads(6)

@time a = CSV.read(
    "C:/data/Performance_All/Performance_2003Q1.txt",
    delim = '|',
    header = false,
    threaded = true
    # reduce memory usage
);
# GC.gc()
# @time metadatas = psavejdf(a, "c:/data/large8.jdf");
# GC.gc()


GC.gc()
@time metadatas = savejdf(a, "c:/data/large8.jdf");
a = nothing
GC.gc()

GC.gc()
@time JLSO.save("c:/data/large8.meta.jlso", metadatas)
GC.gc()

using Revise, JLSO, JDF, DataFrames
@time metadatas = JLSO.load("c:/data/large8.meta.jlso")["data"];
GC.gc()

@time a2 = loadjdf("c:/data/large8.jdf", metadatas);
GC.gc()

@time psavejdf(a2, "c:/data/large8p.dir.jdf");

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

#b = Array(a.FL_DATE);

#b = coalesce.(Array(a[:Column31]), "")
# b = Array(a2[!, :Column18]);
#
# 1884457
# io = iow()
# @time metadata = compress_then_write(b, io)
# # using JLSO
# # JLSO.save("c:/data/metatmp", metadata)
# close(io)
#
#
# using Revise, JDF, CSV, DataFrames
# # using JLSO
# # metadata = JLSO.load("C:/data/metatmp")["data"]
# buffer = rand(UInt8, 30_000_000)
#
# GC.gc()
# io = ior()
# @time oo = column_loader!(buffer, eltype(b), io, metadata);
# close(io)
# GC.gc()
#
# a = String(copy(buffer[1:metadata.string_compressed_bytes]));
#
# all(b.==oo)

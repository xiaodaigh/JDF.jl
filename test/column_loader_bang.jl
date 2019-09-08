using Revise
using JDF, CSV, DataFrames
#a = gf()

@time a = CSV.read("C:/data/Performance_All/Performance_2010Q3.txt", delim = '|', header = false);

#b = Array(a.FL_DATE);

b = coalesce.(Array(a[:Column31]), "")


io = iow()
@time metadata = compress_then_write(b, io)
# using JLSO
# JLSO.save("c:/data/metatmp", metadata)
close(io)


using JDF
using CSV, DataFrames
# using JLSO
# metadata = JLSO.load("C:/data/metatmp")["data"]
buffer = rand(UInt8, 30_000_000)

io = ior()
@time oo = column_loader!(buffer, eltype(b), io, metadata);
close(io)

a = String(copy(buffer[1:metadata.string_compressed_bytes]));

all(b.==oo)

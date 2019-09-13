using Revise
using JDF, CSV, DataFrames
@time a = gf()

# 2G file
# @time a = CSV.read(
#     "C:/data/Performance_All/Performance_2010Q3.txt",
#     delim = '|',
#     header = false
# );

b = Array(a[!, :DEP_TIME]);

io = iow()
@time metadata = compress_then_write(b, io)
close(io)


using Revise
using JDF, CSV, DataFrames
io = ior()
@time oo = column_loader(eltype(b), io, metadata);
close(io)

a = String(copy(buffer[1:metadata.string_compressed_bytes]));

all(b.==oo)

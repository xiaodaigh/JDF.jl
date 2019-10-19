using Revise
using JDF, CSV, DataFrames
@time a = gf()

# 2G file
# @time a = CSV.read(
#     "C:/data/Performance_All/Performance_2010Q3.txt",
#     delim = '|',
#     header = false
# );

b = type_compress(Array(a[!, :ORIGIN]));


io = iow()
@time metadata = compress_then_write(b, io)
close(io)

using Revise
using JDF, CSV, DataFrames
# using JLSO
# metadata = JLSO.load("C:/data/metatmp")["data"]

io = ior()
@time oo = column_loader(metadata.type, io, metadata);
close(io)

all(b.==oo)

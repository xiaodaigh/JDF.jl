using Revise
using JDF, DataFrames
# a = gf()
# savejdf("c:/data/a.jdf", a)
@time a = loadjdf("c:/data/large.jdf")

# 2G file
# @time a = CSV.read(
#     "C:/data/Performance_All/Performance_2010Q3.txt",
#     delim = '|',
#     header = false
# );

@time b = Array(a[:, :Column2]);

@time res = type_compress(b);

all(b .== res)

# ne = findfirst(b .!= b1)
#
# b[ne], res[ne]


using RLEVectors, StatsBase
ar = rle(res)

ar[2]  .= cumsum(ar[2])
rlev= RLEVector(ar...)

all(b .== rlev)


a = CSV.read("d:/data\\AirOnTimeCSV\\airOT199401.csv")

bb = [Array(b) for b in eachcol(a)]

bbc = Blosc.compress(bb[1])


bbc = Blosc.compress([1,2,3])
io = open("io.test", "w")
write(io, bbc)
write(io, bbc)
close(io)


io = open("io.test", "r")
bbc_new  = Array{UInt8,1}(undef, length(bbc))
bbc_new2  = similar(bbc_new)

read!(io, bbc_new)
read!(io, bbc_new2)


close(io)

Blosc.decompress(Int64, bbc_new)
Blosc.decompress(Int64, bbc_new2)



all(Blosc.decompress(Int64, bbc_new) .== [1,2,3])

using CSV, DataFrames, Bloss, Missing
# a = DataFrame(
# 	a = rand(Int32, 100_000_000), b = rand(Float32, 100_000_000)
# 	)

@time a = CSV.read("d:/data/AirOnTimeCSV/airOT199401.csv")

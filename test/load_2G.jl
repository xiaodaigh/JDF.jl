using Revise

using JDF

using CSV, DataFrames, Blosc, JLSO, Base.GC

# use 12 threads
Blosc.set_num_threads(6)

@time a = CSV.read(
"C:/data/Performance_All/Performance_2010Q3.txt",
delim = '|',
header = false,
copycols=true
);
GC.gc()
@time metadatas = savejdf(a, "c:/data/large.jdf");
GC.gc()

@time JLSO.save("c:/data/large.meta.jlso", metadatas)
GC.gc()

using Revise

using JLSO, JDF
@time metadatas = JLSO.load("c:/data/large.meta.jlso")["data"];
GC.gc()

@time a2 = loadjdf("c:/data/large.jdf", metadatas);
GC.gc()

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

#
# io = iow()
# @time strok = compress_then_write(Array(a[:Column3]), io)
# close(io)
#
# io = ior()
# buffer = Vector{UInt8}(undef, 4_000_000);
# @time aok = column_loader!(buffer, Union{Missing, String}, io, strok);
# close(io)


x = "id".*string.(rand(UInt8, 1_000_000))

io = open("c:/data/io.bin", "w")
ncw = write.(Ref(io), x)
close(io)

io = open("c:/data/io.bin", "r")
buffer = Vector{UInt8}(undef, sum(ncw))
readbytes!(io, buffer, sum(ncw))
cloe(io)

ok = String(buffer);

cncw = cumsum(ncw)
j = 0
fn(s, st, en) = begin
    SubString(s, st, en)
end

ssub = Vector{SubString}(undef, length(ncw))

j = 1
for (i,n) in enumerate(ncw)
    global j
    ssub[i] = SubString(ok, j, j + n - 1)
    j+=n
end

@time ok_sub = SubString.(ok, vcat(0, cncw[1:end-1]).+1, cncw)

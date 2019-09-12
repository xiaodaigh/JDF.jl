using Revise
using JDF
using CSV, DataFrames, JLSO

@time a = gf()


@time metadatas = savejdf(a, "c:/data/a.jdf");
JLSO.save("c:/data/a.metadata.jlso", metadatas)

using Revise
using JLSO, DataFrames, JDF
metadatas = JLSO.load("c:/data/a.metadata.jlso")["data"]
@time a2 = loadjdf("c:/data/a.jdf", metadatas);

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

ok(buffer, ::Type{Int}, io, metadata) = begin
	println("")
	#readbytes!(io, buffer, metadata.len)
    #return Blosc.decompress(T, buffer)
end

ok(buffer, ::Type{String}, io::IOStream, metadata) = begin
	println("")
	println("")
	println("-----------------------START: loading string---------------------")
end

ok(1,Int,2,2)
ok(1,String,2,2)

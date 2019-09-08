using Revise
using JDF

a = gf()
b = Array(a.ORIGIN)
io = iow()
metadata = compress_then_write(b, io)
close(io)

io = ior()
oo = column_loader(String, io, metadata)
close(io)

all(b.==oo)

b = Array(a.FL_DATE)
io = iow()
metadata = compress_then_write(b, io)
close(io)

io = ior()
oo = column_loader(metadata.type, io, metadata)
close(io)

all(b.==oo)

@time metadatas = savejdf(a, "c:/data/a.jdf")

@time a2 = loadjdf("c:/data/a.jdf", metadatas)



using LambdaFn
fn = @λ joinpath.(_1, readdir(_1))
files = fn("d:/data/AirOnTimeCSV")

sort!(files, by = x->filesize(x))

@time listdf =reduce(vcat, CSV.read(f) for f in files[1:50]);


using DataFrames, Blosc
@time schema = savejdf(listdf, "d:/data/plsdel.jdf");
@time loadjdf("d:/data/plsdel.jdf", schema);




	bb = [Blosc.compress(Array(b)) for b in eachcol(a)]
	bbc = bb[1])


	bbc = rand(UInt8, 100)
	io = open("io.test", "w")

	w  = Array{Float32,2}(undef,n1,nw)
	read!(fileID, w[:,iw])
	write(io, bbc)

	close(io)

	io = open("io.test", "r")

	bbc_new = read(io, Vector)

	bbc_new  = Array{UInt8,1}(undef, 100)

	read!(io, bbc_new)

	close(io)


N = 100_000_000;
K = 100;

# faster string sort
svec = rand("id".*string.(1:N÷K), N);
using Blosc
# save with no compression
io = open("c:/data/no_compression.bin", "w")
@time ncw = write.(Ref(io), codeunits.(svec));
ncwtot=sum(ncw)
@time ncw_compressed_tot = write(io, Blosc.compress(UInt16.(ncw)))
close(io)



@time res = column_loader(String, io, ncwtot)


io = open("c:/data/no_compression.bin", "r")
@time tmp = Vector{UInt8}(undef, sum(ncw))
@time read!(io, tmp)
close(io)


@time tmp_compressed = Blosc.compress(tmp)
io = open("c:/data/compression.bin", "w")
write(io, tmp_compressed)
close(io)


tot = sum(ncw)
ok(tmp, ncw) = begin
   	cncw = cumsum(ncw)
   	str = unsafe_string(pointer(tmp))
   	getindex.(Ref(str), Colon().(vcat(1, cncw[1:end-1].+1), cncw))
end


io = open("c:/data/bin.bin", "r")
@time tmp = Vector{UInt8}(undef, tot)
@time read!(io, tmp)
@time ok(tmp, ncw)
close(io)


# @time nc = ncodeunits.(svec)
# @time aa = Vector{UInt8}(undef, sum(nc))

# meh!(aa, svec, nc) = begin
# 	start = 1
# 	@time for (n, s) in zip(nc, svec)
# 		aa[1:n] = codeunits(s)
# 		start += n
# 	end
# end

@time meh!(aa, svec, nc)

@time a  = codeunits.(svec)
close(io)

@time ncw = write.(Ref(io), a);

using Blosc
io = open("c:/data/compression.bin", "w")
@time ncw = write.(Ref(io), Blosc.compress.(codeunits.(svec)));
close(io)





stringit(n, io) = begin
    tmp = Vector{UInt8}(undef, n)
    read!(io, tmp)
    tmp
end

tot = sum(ncw)

ok(tmp, ncw) = begin
   	cncw = cumsum(ncw)
   	str = unsafe_string(pointer(tmp))
   	getindex.(Ref(str), Colon().(vcat(1, cncw[1:end-1].+1), cncw))
end


io = open("c:/data/bin.bin", "r")
@time tmp = Vector{UInt8}(undef, tot)
@time read!(io, tmp)
@time ok(tmp, ncw)
close(io)


a = pointer(tmp[1:8])
@time str = unsafe_string(a)
close(io)

io = open("c:/data/bin.bin", "r")
@time svec2 = stringit.(ncw, Ref(io))
close(io)

@time svec2 = unsafe_string.(pointer.(aa))
all(svec2 .== svec)

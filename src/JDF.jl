__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using JLSO: JLSO
using CSV:CSV
using Missings:Missings

export savejdf, loadjdf, extt, gf, iow, ior, compress_then_write
export column_loader, gf2

gf() = begin
	CSV.read("c:/data/AirOnTimeCSV/airOT198710.csv")
end

gf2() = begin
	p = "c:/data/AirOnTimeCSV/"
	f = joinpath.(p, readdir(p))
	sort!(f, by = x->filesize(x), rev=true)
	reduce(vcat, CSV.read.(f[1:100]))
end

iow() = begin
	open("c:/data/bin.bin", "w")
end

ior() = begin
	open("c:/data/bin.bin", "r")
end

some_elm(x) = zero(x)
some_elm(::Type{Missing}) = missing
some_elm(::Type{String}) = ""

compress_then_write(b, io) = compress_then_write(eltype(b), b, io)

compress_then_write(::Type{Missing}, b, io) = (len=0, type=Missing)

compress_then_write(T, b, io) = begin
	bbc = Blosc.compress(b)
	res = length(bbc)
	write(io, bbc)
	return (len=res, type=T)
end

compress_then_write(::Type{String}, b::Array{String}, io) = begin
	# firstly write the string
	# which returns the bytes written
	@time ncw = write.(Ref(io), codeunits.(b));

	# now write the bytes
	bbc = Blosc.compress(ncw)
	@time ncw_compressed = write(io, bbc)

	return (string_compressed_bytes = sum(ncw), string_len_bytes = length(bbc), type = String)
end


hehe(name, b, io) = begin
	println(name)
	compress_then_write(b, io)
end

savejdf(df, outfile) = begin
	"""
		save a DataFrames to the outfile
	"""
	io  = open(outfile, "w")
	bytes = [hehe(name, Array(b), io) for (name, b) in zip(names(df), eachcol(df))]
	close(io)
	(names = names(df), rows = size(df,1), metadatas = bytes)
end

# load the data from file with a schema
loadjdf(path, metadatas) = begin
    io = open(path, "r")
    df = DataFrame()
    i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
    	println(i)
		println(metadata)
    	res = column_loader(metadata.type, io, metadata)
    	if res == nothing
			df[!,name] = Vector{Missing}(missing, metadatas.rows)
		else
    		df[!,name] = res
    	end
		i +=1
    end
 	close(io)
 	df
end

# Extract type in Union{Missing, T}
extt(::Type{Union{Missing, T}}) where T = T

# load bytes bytes from io decompress into type
column_loader(T::Type, io, metadata) = begin
	if metadata.len > 0
		res = Vector{UInt8}(undef, metadata.len)
		read!(io, res)
	    return Blosc.decompress(T, res)
	end
	nothing
end

compress_then_write(::Type{Bool}, b, io) = begin
	b8 = UInt8.(b)
	bbc = Blosc.compress(b8)
	write(io, bbc)
	return (len=length(bbc), type=Bool)
end

column_loader(T::Type{Bool}, io, metadata) = begin
	# Bool are saved as UInt8
	tmp = Vector{UInt8}(undef, metadata.len)
	read!(io, tmp)
	Bool.(Blosc.decompress(UInt8, tmp))
end

compress_then_write(::Type{T}, b, io) where T >: Missing = begin
	print("compress missing: ")
	println(eltype(b))
	S = extt(eltype(b))
	b_S = replace(b, missing => some_elm(S)) |> disallowmissing

	metadata = compress_then_write(S, b_S, io)

	b_m = ismissing.(b)
	metadata2 = compress_then_write(eltype(b_m), b_m, io)

	(Tmeta = metadata, missingmeta = metadata2, type = eltype(b))
end

column_loader(::Type{Union{Missing, T}}, io, metadata) where T = begin
	print("hi")
	# read the content
	Tmeta = metadata.Tmeta

	t = column_loader(Tmeta.type, io, Tmeta) |> allowmissing

	# read the missings
	m = column_loader(
		metadata.missingmeta.type,
		io,
		metadata.missingmeta)

	t[m] .= missing

	t
end

column_loader(::Type{Missing}, io, metadata) = nothing

# load a column
column_loader(::Type{String}, io::IOStream, metadata) = begin
   """
	   metadata should consists of length, compressed byte size of string-lengths,
	   string content lengths
   """
   # load the strings
   tmp = Vector{UInt8}(undef, metadata.string_compressed_bytes)
   read!(io, tmp)

   # next load the lengths
   tmp2 = Vector{UInt8}(undef, metadata.string_len_bytes)
   read!(io, tmp2)

   str_lens  = Blosc.decompress(Int64, tmp2)

   # load every as a one big string
   # this is FAST
   long_str = unsafe_string(pointer(tmp));

   cncw = cumsum(str_lens)

   # break it up into smaller string
   getindex.(Ref(long_str), Colon().(vcat(1, cncw[1:end-1].+1), cncw))
end

end # module

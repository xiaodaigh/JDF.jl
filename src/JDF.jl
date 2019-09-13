__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using JLSO: JLSO
using CSV:CSV
using Missings:Missings
using Base: _string_n
using StatsBase:rle, inverse_rle
using BufferedStreams
using WeakRefStrings
using Blosc

export savejdf, loadjdf, nonmissingtype, gf, iow, ior, compress_then_write
export column_loader!, gf2, psavejdf, column_loader

import Base.Threads.@spawn
import Base: size, show, getindex, setindex!, eltype
import Missings: allowmissing, disallowmissing

export CmpVector

mutable struct CmpVector{T} <: AbstractVector{T}
	compressed::Vector{UInt8}
	value::Vector{T}
	inited::Bool
	size::Union{Tuple, Nothing}
end

CmpVector{T}(compressed::Vector{UInt8}) where T = begin
	CmpVector{T}(compressed, Vector{T}(undef, 0), false, nothing)
end

CmpVector(value::Vector{T}) where T = begin
	CmpVector{T}(Blosc.compress(value), Vector{T}(undef, 0), false, size(value))
end

type_compress(df::DataFrame) = type_compress!.(eachcol(df))

type_compress!(v::Vector{T}) where T <: Union{Int128, Int64, Int32} = compress(df)


Base.eltype(cv::CmpVector{T}) where T = eltype(cv.value)

decompress(cv::CmpVector{T}) where T = begin
	if !cv.inited
		cv.value = Blosc.decompress(T, cv.compressed)
		cv.inited = true
		cv.size = size(cv.value)
	end
	cv.value
end

compress(cv::CmpVector{T}) where T = begin
	if cv.inited
		cv.compressed = Blosc.compress(cv.value)
		cv.inited = false
		cv.value = Vector{T}(undef, 0)
	end
	cv
end

Missings.allowmissing(cv::CmpVector{T}) where T = begin
	decompress(cv)
	allowmissing(cv.value)
end

Missings.disallowmissing(cv::CmpVector{T}) where T = begin
	decompress(cv)
	disallowmissing(cv.value)
end

Base.size(pf::CmpVector{T}) where T = size(decompress(pf))

Base.show(io::IO, A::MIME"text/plain", pf::CmpVector{T}) where T = begin
	if pf.inited
		show(io, A, decompress(pf))
	else
		show(io, A, "Compressed; not shown until first access; or CmpVectors.decompress(cv) is run")
	end
end

Base.getindex(pf::CmpVector{T}, i...) where T = begin
	decompress(pf)[i...]
end

Base.setindex!(cv::CmpVector{T}, i...) where T = begin
	val = decompress(cv)
	setindex!(val, i...)
end

Blosc.set_num_threads(6)

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

compress_then_write(T, b, io) = begin
	bbc = Blosc.compress(b)
	res = length(bbc)
	write(io, bbc)
	return (len=res, type=T)
end

psavejdf(df, outfile) = begin
	"""
		save a DataFrames to the outfile
	"""
	#io  = open(outfile, "w")
    pmetadatas = Any[missing for i in 1:length(names(df))]
    #for (name, b) in zip(names(df), eachcol(df))
	if !isdir(outfile)
		mkpath(outfile)
	end
	ios = BufferedOutputStream.(open.(joinpath.(outfile, string.(names(df))), Ref("w")))

	for i in 1:length(names(df))
        #el = @elapsed push!(metadatas, compress_then_write(Array(b), io))
		#println("Start: "*string(Threads.threadid()))
		pmetadatas[i] = Threads.@spawn compress_then_write(Array(df[!,i]), ios[i])
		#pmetadatas[i] = compress_then_write(Array(df[!,i]), ios[i])
		#println("End: "*string(Threads.threadid()))
        #println("saving $name took $el. Type: $(eltype(Array(b)))")
    end
	#close(io)
	metadatas = fetch.(pmetadatas)
	close.(ios)
	(names = names(df), rows = size(df,1), metadatas = metadatas, pmetadatas = pmetadatas)
end

savejdf(df, outfile) = begin
	"""
		save a DataFrames to the outfile
	"""
	io  = BufferedOutputStream(open(outfile, "w"))
    metadatas = Any[]
    for (name, b) in zip(names(df), eachcol(df))
		# println(name)
		# println(eltype(b))
        el = @elapsed push!(metadatas, compress_then_write(Array(b), io))
        # println("saving $name took $el. Type: $(eltype(Array(b)))")
    end
	close(io)
	(names = names(df), rows = size(df,1), metadatas = metadatas)
end

# figure out from metadata how much space is allocated
get_bytes(metadata) = begin
    if metadata.type == String
        return max(metadata.string_compressed_bytes, metadata.string_len_bytes)
    elseif metadata.type == Missing
        return 0
    elseif metadata.type >: Missing
        return max(get_bytes(metadata.Tmeta), get_bytes(metadata.missingmeta))
    else
        return metadata.len
    end
end

# load the data from file with a schema
loadjdf(path, metadatas) = begin
    io = BufferedInputStream(open(path, "r"))
    df = DataFrame()

	# get the maximum number of bytes needs to read
	bytes_needed = maximum(get_bytes.(metadatas.metadatas))

	# preallocate once
	#read_buffer = Vector{UInt8}(undef, bytes_needed)

    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		# println(name)
		# println(metadata)
		if metadata.type == Missing
			df[!,name] = Vector{Missing}(missing, metadatas.rows)
		else
			el = @elapsed res = column_loader(metadata.type, io, metadata)
    		df[!,name] = res
			# println("$el | loading $name | Type: $(metadata.type)")
    	end
    end
 	close(io)
 	df
end

# load bytes bytes from io decompress into type
column_loader!(buffer, ::Type{T}, io, metadata) where T = begin
	readbytes!(io, buffer, metadata.len)
    #return Blosc.decompress(T, buffer)
	return CmpVector{T}(buffer)
end

column_loader(::Type{T}, io, metadata) where T = begin
	buffer = Vector{UInt8}(undef, metadata.len)
	column_loader!(buffer, T, io, metadata)
end

compress_then_write(::Type{Bool}, b, io) = begin
	b8 = UInt8.(b)
	bbc = Blosc.compress(b8)
	write(io, bbc)
	return (len=length(bbc), type=Bool)
end

column_loader(T::Type{Bool}, io, metadata) = begin
	# Bool are saved as UInt8
	buffer = Vector{UInt8}(undef, metadata.len)
	readbytes!(io, buffer, metadata.len)
	Bool.(Blosc.decompress(UInt8, buffer))
end

compress_then_write(::Type{T}, b, io) where T >: Missing = begin
	S = nonmissingtype(eltype(b))
	b_S = coalesce.(b, some_elm(S))

	metadata = compress_then_write(S, b_S, io)

	b_m = ismissing.(b)
	metadata2 = compress_then_write(eltype(b_m), b_m, io)

	(Tmeta = metadata, missingmeta = metadata2, type = eltype(b))
end

column_loader(::Type{Union{Missing, T}}, io, metadata) where T = begin
	# read the content
	Tmeta = metadata.Tmeta

	t_pre = column_loader(Tmeta.type, io, Tmeta) |> allowmissing

	# read the missings as bool
	m = column_loader(
		Bool,
		io,
		metadata.missingmeta)

	t_pre[m] .= missing
	t_pre
end

# perform a RLE
compress_then_write(::Type{String}, b::Array{String}, io) = begin

	# write the string one by one
	# do a Run-length encoding (RLE)
	previous_b = b[1]
	cnt = 1
	lens = Int[]
	str_lens = Int[]
	for i = 2:length(b)
		if b[i] != previous_b
			push!(str_lens, write(io, previous_b))
			push!(lens, cnt)
			#push!(str_lens, sizeof(previous_b))
			cnt = 0
			previous_b = b[i]
		end
		cnt += 1
	end

	# reach the end: two situation
	# 1) it's a new element, so write it
	# 2) it's an existing element. Also write it
	push!(str_lens, write(io, previous_b))
	push!(lens, cnt)
	#push!(str_lens, sizeof(previous_b))

	@assert sum(lens) == length(b)

	str_lens_compressed = Blosc.compress(UInt32.(str_lens))
	str_lens_bytes = write(io, str_lens_compressed)

	lens_compressed = Blosc.compress(UInt64.(lens))
	rle_bytes = write(io, lens_compressed)

	# return metadata
	return (string_compressed_bytes = sum(str_lens),
		string_len_bytes = str_lens_bytes,
		rle_bytes = rle_bytes,
		rle_len = length(str_lens),
		type = String,
		orig_len = length(b),
		len = sum(str_lens) + str_lens_bytes + rle_bytes)
end


# load a string column
"""
	metadata should consists of length, compressed byte size of string-lengths,
	string content lengths
"""

column_loader(::Type{String}, io, metadata) = begin
	buffer = Vector{UInt8}(undef, metadata.string_compressed_bytes)
	readbytes!(io, buffer, metadata.string_compressed_bytes)
	#return String(buffer)

	# read the string-lengths
	buffer2 = Vector{UInt8}(undef, metadata.string_len_bytes)
	readbytes!(io, buffer2, metadata.string_len_bytes)

	buffer3 = Vector{UInt8}(undef, metadata.rle_bytes)
	readbytes!(io, buffer3, metadata.rle_bytes)

	counts = Blosc.decompress(UInt64, buffer3)

	str_lens = Blosc.decompress(UInt32, buffer2)

	lengths = inverse_rle(str_lens, counts)
	offsets = inverse_rle(vcat(0, cumsum(str_lens[1:end-1])), counts)

	#return (buffer, counts, lengths, offsets)

	#res = StringArray{String, 1}(buffer, vcat(1, cumsum(Blosc.decompress(UInt64, buffer3))[1:end-1]) .-1,  )
	res = StringArray{String, 1}(buffer, offsets, lengths)
end

compress_then_write(::Type{Missing}, b, io) = (orig_len=length(b), len=0, type=Missing)
column_loader(::Type{Missing}, io, metadata) = Vector{Missing}(missing, metadata.orig_len)

end # module

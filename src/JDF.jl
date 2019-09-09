__precompile__(true)
module JDF

using Blosc: Blosc
using DataFrames
using JLSO: JLSO
using CSV:CSV
using Missings:Missings
using Base: _string_n
using StatsBase:rle, inverse_rle

export savejdf, loadjdf, nonmissingtype, gf, iow, ior, compress_then_write
export column_loader!, gf2

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

compress_then_write(::Type{Missing}, b, io) = (len=0, type=Missing)

compress_then_write(T, b, io) = begin
	bbc = Blosc.compress(b)
	res = length(bbc)
	write(io, bbc)
	return (len=res, type=T)
end

savejdf(df, outfile) = begin
	"""
		save a DataFrames to the outfile
	"""
	io  = open(outfile, "w")
    metadatas = Any[]
    for (name, b) in zip(names(df), eachcol(df))
        el = @elapsed push!(metadatas, compress_then_write(Array(b), io))
        println("saving $name took $el. Type: $(eltype(Array(b)))")
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
    io = open(path, "r")
    df = DataFrame()

	# get the maximum number of bytes needs to read
	bytes_needed = maximum(get_bytes.(metadatas.metadatas))

	# preallocate once
	read_buffer = Vector{UInt8}(undef, bytes_needed)

    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		#println(name)
		#println(metadata.type)
		if (metadata.type == String) || (metadata.type == Union{Missing, String})
			@time el = @elapsed res = column_loader!(read_buffer, metadata.type, io, metadata)
			println("$el | loading $name | Type: $(metadata.type)")
		else
			el = @elapsed res = column_loader!(read_buffer, metadata.type, io, metadata)
		end
    	if res == nothing
			df[!,name] = Vector{Missing}(missing, metadatas.rows)
		else
    		df[!,name] = res
    	end
    end
 	close(io)
 	df
end

# load bytes bytes from io decompress into type
column_loader!(buffer, T::Type, io, metadata) = begin
	if metadata.len > 0
		readbytes!(io, buffer, metadata.len)
	    return Blosc.decompress(T, buffer)
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
	buffer = Vector{UInt8}(undef, metadata.len)
	readbytes!(io, buffer, metadata.len)
	Bool.(Blosc.decompress(UInt8, buffer))
end

column_loader!(buffer, T::Type{Bool}, io, metadata) = begin
	# Bool are saved as UInt8
	read!(io, buffer)
	res = Blosc.decompress(UInt8, buffer)
	Bool.(res)
end


compress_then_write(::Type{T}, b, io) where T >: Missing = begin
	S = nonmissingtype(eltype(b))
	b_S = coalesce.(b, some_elm(S))

	metadata = compress_then_write(S, b_S, io)

	b_m = ismissing.(b)
	metadata2 = compress_then_write(eltype(b_m), b_m, io)

	(Tmeta = metadata, missingmeta = metadata2, type = eltype(b))
end


column_loader!(buffer, ::Type{Union{Missing, T}}, io, metadata) where T = begin
	# read the content
	Tmeta = metadata.Tmeta

	t_pre = column_loader!(buffer, Tmeta.type, io, Tmeta)
	t = t_pre |> allowmissing
	# read the missings as bool
	m = column_loader(
		Bool,
		io,
		metadata.missingmeta)
	#return t_pre
	t[m] .= missing
	t
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

	str_lens_compressed = Blosc.compress(str_lens)
	str_lens_bytes = write(io, str_lens_compressed)

	lens_compressed = Blosc.compress(lens)
	rle_bytes = write(io, lens_compressed)

	# return metadata
	return (string_compressed_bytes = sum(str_lens),
		string_len_bytes = str_lens_bytes,
		rle_bytes = rle_bytes,
		rle_len = length(str_lens),
		type = String)
end


# load a string column
"""
	metadata should consists of length, compressed byte size of string-lengths,
	string content lengths
"""
column_loader!(buffer, ::Type{String}, io::IOStream, metadata) = begin
	println("")
	println("")
	println("-----------------------START: loading string---------------------")

	print("1. read strings into buffer: ")
	# read the string-lengths
	@time readbytes!(io, buffer, metadata.string_compressed_bytes)
	long_str = String(copy(buffer[1:metadata.string_compressed_bytes]))

	print("2. read string lengths: ")
	# read the string-lengths
	@time readbytes!(io, buffer, metadata.string_len_bytes)
	str_lens = Blosc.decompress(Int, buffer)

	print("3.decompress count buffer: ")
	@time readbytes!(io, buffer, metadata.rle_bytes)
	@time counts = Blosc.decompress(Int, buffer)

	# read the strings-lengths
	print("4. inverse sle")
	@time rle_substrings = Vector{SubString{String}}(undef, length(counts))
	j = 1
	@time for (i, s) in enumerate(str_lens)
		rle_substrings[i] = SubString(long_str, j, j + s - 1)
		j += s
	end
	@time fnl_result = inverse_rle(rle_substrings, counts)
	return fnl_result



	# # loop
	# i = 1
	# #j = 0
	# ptr_to_string_in_bytes = pointer(buffer)
	# # this is to ensure that the pointers are all different
	# #return str_lens
	# print("4. make it into string ")
	# @time long_str = String(copy(buffer[1:metadata.string_compressed_bytes]));
	# # @time reconstituted_strings = [Base._string_n(s) for s in str_lens]
	# # return reconstituted_strings
	# print("5. pre alllocate ")
	# off = 1 ;
	# #@time substrings = [(nxt = off+len; s = SubString(long_str, off, nxt-1); off = nxt; s) for len in str_lens]
	# @time substrings = Vector{SubString{String}}(undef, length(str_lens))
	# j = 1
	# print("6. set strings ")
	# @time for (i,s) in enumerate(str_lens)
	# 	substrings[i] = SubString(long_str, j, j + s - 1)
	# 	j += s
	# end
	# @time for string_byte_size in str_lens
	# 	unsafe_copyto!(
	# 		reconstituted_strings[i] |> pointer,
	# 		ptr_to_string_in_bytes,
	# 		string_byte_size)
	# 	ptr_to_string_in_bytes += string_byte_size
	#
	#     i += 1
	# end
	println("-----------------------EMD: loading string-----------------------")
	substrings
	#reconstituted_strings
end

column_loader!(buffer, ::Type{Missing}, io, metadata) = nothing

# struct LongStringVector{String} <:AbstractVector{String}
# 	long_str::String
# 	cum_ind::Vector{Int}
# end
#
# length(a::LongStringVector) = length(a.cum_ind)
# get_index(a::LongStringVector, ind) = begin
# 	if ind == 1
# 		long_str[1:a.cum_ind[1]]
# 	else
# 		long_str[a.cum_ind[ind-1]+1:a.cum_ind[ind]]
# 	end
# end
# size(a::LongStringVector) = size(a.long_str)

end # module

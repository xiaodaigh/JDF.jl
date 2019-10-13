# TODO allow int to uint
type_compress!(df::DataFrame; compress_float = false, verbose = false) = begin
	for n in names(df)
		if verbose
			println("Compressing $n")
		end
		#vec = Vector(df[!,n])
		if (eltype(vec) != Float64) || compress_float
			df[!,n] = type_compress(vec)
		else
			df[!,n] = type_compress(df[!,n])
		end
	end
	df
end

type_compress(v::Vector{T}) where T <: Union{Int128, Int64, Int32, Int16} = begin
	min1, max1 = extrema(v)

	if typemin(Int8) <= min1 && max1 <= typemax(Int8)
		return Int8.(v)
	elseif typemin(Int16) <= min1 && max1 <= typemax(Int16)
		return Int16.(v)
	elseif typemin(Int32) <= min1 && max1 <= typemax(Int32)
		return Int32.(v)
	elseif typemin(Int64) <= min1 && max1 <= typemax(Int64)
		return Int64.(v)
	end
end

type_compress(v::Vector{T}) where T <: Union{UInt128, UInt64, UInt32, UInt16} = begin
	max1 = maximum(v)

	if max1 <= typemax(UInt8)
		return UInt8.(v)
	elseif max1 <= typemax(UInt16)
		return UInt16.(v)
	elseif max1 <= typemax(UInt32)
		return UInt32.(v)
	elseif max1 <= typemax(UInt64)
		return UInt64.(v)
	end
end

type_compress(v::Vector{Union{Missing, T}}) where T <: Union{UInt128, UInt64, UInt32, UInt16} = begin
	max1 = maximum(skipmissing(v))

	if max1 <= typemax(UInt8)
		return Vector{Union{Missing, UInt8}}(v)
	elseif max1 <= typemax(UInt16)
		return Vector{Union{Missing, UInt16}}(v)
	elseif max1 <= typemax(UInt32)
		return Vector{Union{Missing, UInt32}}(v)
	elseif max1 <= typemax(UInt64)
		return Vector{Union{Missing, UInt64}}(v)
	end
end

type_compress(v::Vector{Union{Missing, T}}) where T <: Union{Int128, Int64, Int32, Int16} = begin
	min1, max1 = extrema(skipmissing(v))

	if typemin(Int8) <= min1 && max1 <= typemax(Int8)
		return Vector{Union{Missing, Int8}}(v)
	elseif typemin(Int16) <= min1 && max1 <= typemax(Int16)
		return Vector{Union{Missing, Int16}}(v)
	elseif typemin(Int32) <= min1 && max1 <= typemax(Int32)
		return Vector{Union{Missing, Int32}}(v)
	elseif typemin(Int64) <= min1 && max1 <= typemax(Int64)
		return Vector{Union{Missing, Int64}}(v)
	end
end

type_compress(v::Vector{Float64}) = Vector{Float32}(v)
type_compress(v::Vector{Union{Missing, Float64}}) = Vector{Union{Missing, Float32}}(v)


type_compress(v::Vector{String}) = begin
	# TODO recommend a PooledString if necessary
	# use some heuristic to decide whether to compress

	## if the number of unique elements is predicted to be less than typemax(UInt16) + 1
	## then we can use categorical array to compress them
	sv = countmap(sample(v, 888))

	# Estimate the number of unique items
	# https://stats.stackexchange.com/questions/19014/how-can-i-estimate-unique-occurrence-counts-from-a-random-sampling-of-data
	u1 = length([key for (key, value) in sv if value == 1])
	u = length(sv)
	# if the estimated number of estimtes is less than about 32000 then it's
	# worth putting it into a categorical array
	if u + u1/888 * (length(v) - 888) < typemax(UInt16) + 1
		return categorical(v)
		# rlev = rle(categorical(v))
		# rlev[2] .= cumsum(rlev[2])
		# return RLEVector(rlev...)
	end

	# check if the string is more compact at RLE level
	rlev = rle(v)

	# firstly check if the rlev can be whittle down further
	offset_dict = Dict{String, UInt}()

	# an IOBuffer to build up the string buffer
	long_string_builder = IOBuffer()
	offset_array_builder = IOBuffer()
	string_len_builder = IOBuffer()

	values = rlev[1]
	offset_upto = 0

	# add the first entry
	offset_dict[values[1]] = offset_upto

	write(offset_array_builder, offset_upto)
	offset_update = write(long_string_builder, values[1])
	write(string_len_builder, offset_update)

	offset_upto += offset_update

	for v in values[2:end]
		if !haskey(offset_dict, v)
			write(offset_array_builder, offset_upto)
			offset_update = write(long_string_builder, v)
			write(string_len_builder, offset_update)

			# update the offset_upto for next one
			offset_dict[v] = offset_upto
			offset_upto += offset_update
			# println(v, " : ", offset_dict[v])
		else
			# If the string doesn't existing then obtain the offset from the
			# offset_dict.
			# There is no need to write it to the string aagain because it's
			# already there

			offset_old = offset_dict[v]
			# still write the lengths
			write(string_len_builder, sizeof(v))
			# still write the offset
			write(offset_array_builder, offset_old)
		end
	end

	#long_str =String(take!(long_string_builder))
	offsets = inverse_rle(reinterpret(Int, take!(offset_array_builder)), rlev[2])
	lengths = inverse_rle(reinterpret(Int, take!(string_len_builder  )), rlev[2])

	buffer = take!(long_string_builder)

	close(long_string_builder)
	close(offset_array_builder)
	close(string_len_builder)

	#return String(buffer), offsets, lengths
	#return (StringArray{String, 1}(buffer, offsets, lengths), String(buffer), offsets, lengths)
	return StringArray{String, 1}(buffer, offsets, lengths)

end

type_compress(v::Vector{Missing}) = v

type_compress(v::CategoricalVector) where {T, IntType}  = compress(v)

type_compress(v) = begin
	# println("The compression for $(typeof(v)) is not yet supported. No compression is performed. Submit an idea if you think JDF should support it: https://github.com/xiaodaigh/JDF.jl/issues")
	v
end

if false

end

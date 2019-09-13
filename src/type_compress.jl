type_compress!(df::DataFrame; verbose = false) = begin
	for n in names(df)
		if verbose
			println("Compressing $n")
		end
		df[!,n] = type_compress(Array(df[!,n]))
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
	min1, max1 = extrema(v)

	if typemin(UInt8) <= min1 && max1 <= typemax(UInt8)
		return UInt8.(v)
	elseif typemin(UInt16) <= min1 && max1 <= typemax(UInt16)
		return UInt16.(v)
	elseif typemin(UInt32) <= min1 && max1 <= typemax(UInt32)
		return UInt32.(v)
	elseif typemin(UInt64) <= min1 && max1 <= typemax(UInt64)
		return UInt64.(v)
	end
end

type_compress(v::Vector{Union{Missing, T}}) where T <: Union{UInt128, UInt64, UInt32, UInt16} = begin
	min1, max1 = extrema(skipmissing(v))

	if typemin(UInt8) <= min1 && max1 <= typemax(UInt8)
		return Vector{Union{Missing, UInt8}}(v)
	elseif typemin(UInt16) <= min1 && max1 <= typemax(UInt16)
		return Vector{Union{Missing, UInt16}}(v)
	elseif typemin(UInt32) <= min1 && max1 <= typemax(UInt32)
		return Vector{Union{Missing, UInt32}}(v)
	elseif typemin(UInt64) <= min1 && max1 <= typemax(UInt64)
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
type_compress(v::Vector{Union{Missing, Float64}}) = Vector{Union{Missing, Float64}}(v)

type_compress(v) = v

# load the data from file with a schema
sloadjdf(indir; verbose = false) = begin
	metadatas = deserialize(joinpath(indir,"metadata.jls"))

    df = DataFrame()

	# get the maximum number of bytes needs to read
	bytes_needed = maximum(get_bytes.(metadatas.metadatas))

	# preallocate once
	read_buffer = Vector{UInt8}(undef, bytes_needed)

    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		# println(name)
		# println(metadata)
		io = BufferedInputStream(open(joinpath(indir,string(name)), "r"))
		if metadata.type == Missing
			df[!,name] = Vector{Missing}(missing, metadatas.rows)
		else
			el = @elapsed res = column_loader!(read_buffer, metadata.type, io, metadata)
    		df[!,name] = res
			# println("$el | loading $name | Type: $(metadata.type)")
    	end
		close(io)
    end
 	df
end

loadjdf(indir; verbose = false) = begin
	if VERSION < v"1.3.0-rc1.0"
		return sloadjdf(indir, verbose = verbose)
	end

	if verbose
		println("loading $indir in parallel")
	end

	metadatas = deserialize(joinpath(indir,"metadata.jls"))

    df = DataFrame()

	# get the maximum number of bytes needs to read
	bytes_needed = maximum(get_bytes.(metadatas.metadatas))


	results = Any[]
	i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		# println(name)
		# println(metadata)
		io = BufferedInputStream(open(joinpath(indir,string(name)), "r"))
		#el = @elapsed res = column_loader!(read_buffer, metadata.type, io, metadata)
		#df[!,name] = column_loader!(read_buffer, metadata.type, io, metadata)
		if VERSION >= v"1.3.0-rc1.0"
			push!(results, (name = name, io = io, task = @spawn column_loader(metadata.type, io, metadata)))
		end
		# if i == 6
		# 	return results
		# end
		i+=1

		# println("$el | loading $name | Type: $(metadata.type)")
    end
	# return results
	# return metadatas
	for result in results
		if verbose
			println("Extracting $(result.name)")
		end
		new_result = fetch(result.task)
		#println(first(new_result))
		if new_result == nothing
			df[!, result.name] = Vector{Missing}(missing, metadatas.rows)
		else
			df[!, result.name] = new_result
		end
		close(result.io)
	end
 	df
end

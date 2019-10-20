"""
    loadjdf(indir, verbose = true)

    loadjdf(indir, cols = Vector{Symbol}, verbose = true)

Load a `DataFrame` from JDF saved at `outdir`. On Julia > v1.3, a multithreaded
version is used.



"""
loadjdf(indir; cols = Symbol[], verbose = false) = begin
	if VERSION < v"1.3.0-rc1.0"
		return sloadjdf(indir, verbose = verbose)
	end

	if verbose
		println("loading $indir in parallel")
	end

	metadatas = deserialize(joinpath(indir,"metadata.jls"))
	if length(cols) == 0
		cols = metadatas.names
	else
		scmn = setdiff(cols, metadatas.names)
		if length(scmn) > 0
			throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
		end
	end

    df = DataFrame()

	# get the maximum number of bytes needs to read
	# bytes_needed = maximum(get_bytes.(metadatas.metadatas))

	results = Vector{Any}(undef, length(cols))

	# rate limit channel
	c1 = Channel{Bool}(Threads.nthreads())
	atexit(()->close(c1))

	i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		if name in cols
			put!(c1, true)
			results[i] = @spawn begin
				io = BufferedInputStream(open(joinpath(indir,string(name)), "r"))
				new_result = column_loader(metadata.type, io, metadata)
				close(io)
				(name = name, task = new_result)
			end
			take!(c1)
			i+=1
		end
    end

	# run this serially
	for result in results
		if verbose
			println("Extracting $(result.name)")
		end

		new_result = fetch(result).task
		colname = fetch(result).name
		if new_result == nothing
			df[!, colname] = Vector{Missing}(missing, metadatas.rows)
		else
			df[!, colname] = new_result
		end
	end
 	df
end

loadjdf(jdf::JDFFile) = loadjdf(jdf.path)
sloadjdf(jdf::JDFFile) = sloadjdf(jdf.path)

# load the data from file with a schema
sloadjdf(indir; cols = Symbol[], verbose = false) = begin
	metadatas = deserialize(joinpath(indir,"metadata.jls"))

	if length(cols) == 0
		cols = metadatas.names
	else
		scmn = setdiff(cols, metadatas.names)
		if length(scmn) > 0
			throw("columns $(reduce((x,y) -> string(x) * ", " * string(y), scmn)) are not available, please ensure you have spelt them correctly")
		end
	end

    df = DataFrame()

	# get the maximum number of bytes needs to read
	# bytes_needed = maximum(get_bytes.(metadatas.metadatas))

	# rate limit channel
	#results = Vector{Any}(undef, length(metadatas.names))
	results = Vector{Any}(undef, length(cols))

	i = 1
    for (name, metadata) in zip(metadatas.names, metadatas.metadatas)
		if name in cols
			if verbose
				println("Loading $name")
			end
			results[i] = begin
				io = BufferedInputStream(open(joinpath(indir,string(name)), "r"))
				new_result = column_loader(metadata.type, io, metadata)
				close(io)
				(name = name, task = new_result)
			end
			i+=1
		end
    end

	# run this serially
	for result in results
		if verbose
			println("Extracting $(result.name)")
			println(result.task)
		end

		new_result = result.task
		colname = result.name
		if new_result == nothing
			df[!, colname] = Vector{Missing}(missing, metadatas.rows)
		else
			df[!, colname] = new_result
		end
	end
 	df
end

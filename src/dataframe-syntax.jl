# ######################
# A DataFrame way to get file

import Base: getindex, view

export getindex

Base.getindex(file::JDFFile, rows, col::Symbol) = begin
	loadjdf(file; cols = [col])[rows, 1]
end

Base.getindex(file::JDFFile, rows, cols::AbstractVector{Symbol}) = begin
	loadjdf(file; cols = cols)[rows, :]
end

Base.view(file::JDFFile, rows, cols) = getindex(file, rows, cols)

getindex(file::JDFFile, rows, cols) = loadjdf(file)[rows, cols]

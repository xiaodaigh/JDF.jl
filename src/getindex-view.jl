# ######################
# A DataFrame way to get file

import Base: getindex, view

export getindex

function Base.getindex(file::JDFFile, rows, col::String)
    JDF.load(file; cols = [col])[rows, 1]
end

Base.getindex(file::JDFFile, rows, cols::AbstractVector{String}) = begin
    JDF.load(file; cols = cols)[rows, :]
end

Base.view(file::JDFFile, rows, cols) = getindex(file, rows, cols)

getindex(file::JDFFile, rows, cols) = JDF.load(file)[rows, cols]

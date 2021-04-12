export JDFFile, @jdf_str, path, getindex

import Base: getindex, view


"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you use with methods like  `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
size(jdf"plsdel.jdf") # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

# clean up
rm("plsdel.jdf", force = true, recursive = true)
"""
struct JDFFile{T<:AbstractString}
    path::T
end

"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you can apply `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
ncol(jdf"plsdel.jdf") # 2
size(jdf"plsdel.jdf") # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

# clean up
rm("plsdel.jdf", force = true, recursive = true)
"""
macro jdf_str(path)
    return :(JDFFile($path))
end

"""
    path(jdf::JDFFile)

Return the path of the JDF
"""
path(jdf) = getfield(jdf, :path)


function Base.getindex(file::JDFFile, rows, col::String)
    # TODO make it load from column loader for faster access
    getfield(JDF.load(file; cols = [col]), Symbol(col))[rows]
end

function Base.getindex(file::JDFFile, rows, cols::AbstractVector{String})
    JDF.load(file; cols = cols)[rows, :]
end

Base.view(file::JDFFile, rows, cols) = getindex(file, rows, cols)

getindex(file::JDFFile, rows, cols) = JDF.load(file)[rows, cols]

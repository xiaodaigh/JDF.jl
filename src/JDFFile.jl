export JDFFile, @jdf_str, path

"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you can apply `nrow`, `ncol`, `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
nrow(jdf"plsdel.jdf") # 3
ncol(jdf"plsdel.jdf") # 2
size(jdf"plsdel.jdf") # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

size(jdf"plsdel.jdf", 1) # (2, 3)

# clean up
rm("plsdel.jdf", force = true, recursive = true)
"""
struct JDFFile{T <: AbstractString}
    path::T
end

"""
    jdf"path/to/JDFfile.jdf"

    JDFFile("path/to/JDFfile.jdf")

Define a JDF file, which you can apply `nrow`, `ncol`, `names` and `size`.

## Example
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]
nrow(jdf"plsdel.jdf") # 3
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

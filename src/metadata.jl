"""
    JDF.metadata(indir)

Load the metadata associated with the JDF in `indir`
"""
jdfmetadata(indir) = begin
    open(joinpath(indir,"metadata.jls")) do io
        deserialize(io)
    end
end

"""
    JDF.metadata(indir)

Load the metadata associated with the JDF in `indir`
"""
metadata(jdf::JDFFile) = jdfmetadata(jdf.path)

"""
    JDF.names(indir)

Load the column names associated with the JDF in `indir`

# Examples
```julia
using JDF, DataFrames
savejdf(DataFrame(a = 1:3, b = 1:3), "plsdel.jdf")
JDF.names("plsdel.jdf")
```
"""
names(jdf::JDFFile) = metadata(jdf).names
# names(indir) = jdfmetadata(indir).names

"""
    JDF.nrow(indir)

Returns number of rows of a JDF
"""
nrow(jdf::JDFFile) = metadata(jdf).rows
# nrow(indir) = metadata(indir).rows


"""
    JDF.ncol(indir)

Returns number of rows of a JDF
"""
ncol(jdf::JDFFile) = metadata(jdf).names |> length
# ncol(indir) = metadata(indir).names |> length


"""
    JDF.size(indir)

Returns the JDF's `size`
"""
# JDF.size(indir) = begin
#     m = metadata(indir)
#     (m.rows, length(m.names))
# end
JDF.size(jdf::JDFFile) = begin
     m = metadata(jdf)
     (m.rows, length(m.names))
end

# JDF.size(path::AbstractString) = JDF.size(JDFFile(path))


# JDF.size(indir, v) = JDF.size(indir, Val(v))
JDF.size(jdf::JDFFile, val) = JDF.size(jdf, Val(val))

"""
    JDF.size(indir, 1)

Returns the number of rows
"""
JDF.size(jdf::JDFFile, ::Val{1}) = nrow(jdf)

"""
    JDF.size(indir, 2)

Returns the number of columns
"""
#JDF.size(indir, ::Val{2}) = ncol(indir)
JDF.size(jdf::JDFFile, ::Val{2}) = ncol(jdf)

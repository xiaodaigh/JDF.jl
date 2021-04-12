"""
    JDF.metadata(indir)

Load the metadata associated with the JDF in `indir`
"""
jdfmetadata(indir) = begin
    open(joinpath(indir, "metadata.jls")) do io
        deserialize(io)
    end
end

"""
    JDF.metadata(indir)

Load the metadata associated with the JDF in `indir`
"""
metadata(jdf::JDFFile) = jdfmetadata(path(jdf))



"""
    JDF.names(indir)

Load the column names associated with the JDF in `indir`

# Examples
```julia
using JDF, DataFrames
JDF.save(DataFrame(a = 1:3, b = 1:3), "plsdel.jdf")
JDF.names("plsdel.jdf")
```
"""
names(jdf::JDFFile) = metadata(jdf).names
# names(indir) = jdfmetadata(indir).names


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

# JDF.size(indir, v) = JDF.size(indir, Val(v))
JDF.size(jdf::JDFFile, val) = JDF.size(jdf)[val]

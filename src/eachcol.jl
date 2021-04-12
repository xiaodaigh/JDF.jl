import Base: iterate, length, eachcol

struct JDFFileColIterator
    jdf::JDFFile
    cols::Vector{Union{String,Symbol}}
end

eachcol(jdf::JDFFile) = JDFFileColIterator(jdf, names(jdf))

Base.length(jdf::JDFFileColIterator) = length(jdf.cols)

function Base.iterate(jdf::JDFFileColIterator, state = 1)
    if state > length(jdf.cols)
        return nothing
    end

    # TODO isoloate this into a column loader

    indir = path(jdf.jdf)
    # load the metadatas
    metadatas = open(joinpath(indir, "metadata.jls")) do io
        deserialize(io)
    end

    name = metadatas.names[state]
    metadata = metadatas.metadatas[state]

    io = BufferedInputStream(open(joinpath(indir, string(name)), "r"))
    result = column_loader(metadata.type, io, metadata)
    close(io)
    return (result, state+1)
end

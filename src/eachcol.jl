import Base: iterate, length, eachcol

struct JDFFileColIterator
    jdf::JDFFile
    cols::Vector{Union{String,Symbol}}
end

eachcol(jdf::JDFFile) = JDFFileColIterator(jdf, names(jdf))

Base.length(jdf::JDFFileColIterator) = length(jdf.cols)

Base.iterate(jdf::JDFFileColIterator, state = 1) = begin
    if state > length(jdf.cols)
        return nothing
    end

    res1 = sload(jdf.jdf, cols = [jdf.cols[state]])[1]
    return (res1, state + 1)
end

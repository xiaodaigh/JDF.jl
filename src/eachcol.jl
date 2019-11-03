# if VERSION < v"1.1"
#     import Compat:eachcol
# else
    import DataFrames:eachcol
# end

import Base: iterate, length

struct JDFFileColIterator
    jdf::JDFFile
    cols::Vector{Symbol}
end

eachcol(jdf::JDFFile) = JDFFileColIterator(jdf, names(jdf))

Base.length(jdf::JDFFileColIterator) = length(jdf.cols)

Base.iterate(jdf::JDFFileColIterator, state = 1) = begin
    if state > length(jdf.cols)
        return nothing
    end

    res1 = sloadjdf(jdf.jdf, cols=[jdf.cols[state]])[!,1]
    return (res1, state + 1)
end

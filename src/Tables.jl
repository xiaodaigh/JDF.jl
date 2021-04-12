import Tables: rows, columns, istable, rowaccess, columnaccess, schema, Schema

import Base: propertynames, getproperty

export istable, columns

istable(::Type{JDFFile}) = true
istable(::JDFFile) = true

rowaccess(::JDFFile) = false
columnaccess(::JDFFile) = true

rowaccess(::Type{<:JDFFile}) = false
columnaccess(::Type{<:JDFFile}) = true

propertynames(jdf::JDFFile) = names(jdf)

getproperty(jdf::JDFFile, col::Symbol) =  JDF.load(jdf; cols = [col]).columns[col]

schema(jdf::JDFFile) = begin
    meta = metadata(jdf)
    Schema(meta.names, map(x -> x.type, meta.metadatas))
end

columns(jdf::JDFFile) = jdf

# this is the table type specific to JDF
struct Table
    columns::NamedTuple
end


nrow(t::Table) = length(t.columns[1])

ncol(t::Table) = length(t.columns)

Tables.columns(t::Table) = t.columns

Tables.istable(t::Table) = true

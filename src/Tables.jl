import Tables: rows, columns, istable, rowaccess, columnaccess, schema, Schema

import Base: propertynames, getproperty

export istable

Tables.istable(::Type{JDFFile}) = true
Tables.istable(::JDFFile) = true

rowaccess(::JDFFile) = false
columnaccess(::JDFFile) = true

rowaccess(::Type{<:JDFFile}) = false
columnaccess(::Type{<:JDFFile}) = true

propertynames(jdf::JDFFile) = names(jdf)

getproperty(jdf::JDFFile, col::Symbol) = jdf[!, col]

schema(jdf::JDFFile) = begin
     meta  = metadata(jdf)
     Schema(meta.names, map(x->x.type, meta.metadatas))
end

columns(jdf::JDFFile) = jdf

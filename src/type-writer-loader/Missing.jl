# the dispatch for Union{T, Missing}
# 1. comporess the missing
# 2. and also load the missing
compress_then_write(b::Vector{Union{T, Missing}}, io) where T = begin
    S = nonmissingtype(eltype(b))
    b_S = coalesce.(b, some_elm(S))

    metadata = compress_then_write(b_S, io)

    b_m = ismissing.(b)
    metadata2 = compress_then_write(b_m, io)

    (
     Tmeta = metadata,
     missingmeta = metadata2,
     type = eltype(b),
     len = max(metadata.len, metadata2.len),
    )
end

# just write it out as missing
compress_then_write(b::Vector{Missing}, io) = (len = 0, type = Missing)

column_loader!(buffer, ::Type{Union{Missing,T}}, io, metadata) where {T} = begin
    # read the content
    Tmeta = metadata.Tmeta

    t_pre = column_loader!(buffer, Tmeta.type, io, Tmeta) |> allowmissing
    #t = t_pre
    # read the missings as bool
    m = column_loader(Bool, io, metadata.missingmeta)
    #return t_pre
    t_pre[m] .= missing
    t_pre
end

column_loader!(buffer, ::Type{Missing}, io, metadata) = nothing

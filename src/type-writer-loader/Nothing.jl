some_elm(::Type{Nothing}) = nothing

# just write it out as nothing
compress_then_write(b::Vector{Nothing}, io) = (len = 0, type = Nothing, orig_len = length(b))

# the dispatch for Union{T, Missing}
# 1. comporess the missing
# 2. and also load the missing
compress_then_write(b::Vector{Union{T, Nothing}}, io) where T = begin
    #S = nonmissingtype(eltype(b))
    b_S = T[isnothing(b) ? some_elm(T) : b for b in b]

    metadata = compress_then_write(b_S, io)

    b_m = isnothing.(b)
    metadata2 = compress_then_write(b_m, io)

    (
     len = max(metadata.len, metadata2.len),
     type = T,
     metadata = metadata,
     missingmeta = metadata2,
    )
end


column_loader!(buffer, ::Type{Union{Nothing,T}}, io, metadata) where {T} = begin
    # read the content
    Tmeta = metadata.metadata

    t_pre = Vector{Union{Nothing, T}}(column_loader!(buffer, Tmeta.type, io, Tmeta))
    m = column_loader(Bool, io, metadata.missingmeta)

    t_pre[m] .= nothing
    t_pre
end

column_loader!(_, ::Type{Nothing}, _, metadata) = Vector{Nothing}(nothing, metadata.orig_len)

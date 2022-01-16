using Missings: allowmissing

some_elm(::Type{Missing}) = missing

# the dispatch for Union{T, Missing}
# 1. compress the missing
# 2. and also load the missing
function compress_then_write(b::Vector{Union{T,Missing}}, io) where {T}
    b_S = coalesce.(b, some_elm(T))

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
# notice how io is not needed since nothing need to be written
compress_then_write(b::Vector{Missing}, _) =
    (len = 0, type = Missing, orig_len = length(b))

function column_loader!(buffer, ::Type{Union{Missing,T}}, io, metadata) where {T}
    # read the content
    Tmeta = metadata.Tmeta

    t_pre = column_loader!(buffer, Tmeta.type, io, Tmeta) |> allowmissing

    # read the missings as bool
    m = column_loader(Bool, io, metadata.missingmeta)

    t_pre[m] .= missing
    t_pre
end

column_loader!(_, ::Type{Missing}, io, metadata) =
    Vector{Missing}(missing, metadata.orig_len)

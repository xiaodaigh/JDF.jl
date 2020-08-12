using DataAPI, PooledArrays

function compress_then_write(b::PooledVector, io)
    compress_refs = compress_then_write(b.refs, io)
    compress_poolindex = compress_then_write(b.pool, io)

    (type = PooledVector, refs = compress_refs, poolindex = compress_poolindex)
end

function column_loader(b::Type{PooledVector}, io, metadata)
    refs_meta = metadata.refs
    pi_meta = metadata.poolindex
    ref = column_loader(refs_meta.type, io, refs_meta)
    poolindex = column_loader(pi_meta.type, io, pi_meta)


    # TODO more efficient pooledArray construction
    # PooledVector{pi_meta.type}(ref, CategoricalPool{eltype(poolindex), eltype(ref)}(Array(poolindex)))

    ref = max.(1, min.(length(poolindex), ref))
    PooledArray(poolindex[ref])
end

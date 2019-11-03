struct IsBitsType
    val::Bool
    IsBitsType(v::Bool) = new(v)
    IsBitsType(v) = IsBitsType(isbits(v))
    IsBitsType(v::AbstractVector) = IsBitsType(v[end])
end

# save symbols
compress_then_write(b::Vector{ZonedDateTime}, io) = begin
    string_meta = compress_then_write(String.(b), io)
    (type = ZonedDateTime, len = string_meta.len, metadata = string_meta)
end

# load a ZonedDateTime column
column_loader(::Type{ZonedDateTime}, io, metadata) = begin
    strs = column_loader(String, io, metadata.metadata)
    ZonedDateTime.(strs)
end

column_loader!(_, ::Type{ZonedDateTime}, io, metadata) = begin
    column_loader(ZonedDateTime, io, metadata)
end

some_elm(::Type{ZonedDateTime}) = ZonedDateTime(1984, 1, 5, tz"Asia/Shanghai")

if false
a = some_elm(ZonedDateTime)

ar = Vector{ZonedDateTime}(undef, 10)
ar .= a

fieldnames(typeof(a))

typeof(a.utc_datetime)
typeof(a.utc_datetime) |> isbits

fieldnames(typeof(a.utc_datetime))

a.utc_datetime.instant
a.utc_datetime.instant |> isbits

a1 = map(ar) do x
    x.utc_datetime
end

a1[1] |> isbits

open("c:/data/plsdel.io", "w") do io
    compress_then_write(a1, io)
end

typeof(a.timezone)
typeof(a.timezone) |> isbits
fieldnames(a.timezone |> typeof)

a.timezone.name

typeof(a.timezone.name)

typeof(a.timezone.transitions)
typeof(a.timezone.transitions) |> isbits
a.timezone.transitions[1]
a.timezone.transitions[1] |> isbits
fieldnames(typeof(a.timezone.transitions[1]))

a.timezone.transitions[1].utc_datetime
a.timezone.transitions[1].utc_datetime |> typeof
a.timezone.transitions[1].zone |> typeof

typeof(a.timezone.cutoff)
typeof(a.timezone.cutoff) |> isbits


typeof(a.zone)
typeof(a.zone) |> isbits


end

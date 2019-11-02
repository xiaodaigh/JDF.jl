#using Revise
using Test
using JDF, TimeZones, DataFrames

a = some_elm(ZonedDateTime)

ar = Vector{ZonedDateTime}(undef, 10)
ar .= a

meta = open("plsdel.io", "w") do io
  compress_then_write(ar, io)
end

ar_loaded = open("plsdel.io") do io
  column_loader(meta.type, io, meta)
end

@test ar == ar_loaded

df = DataFrame(ar = ar)

savejdf(df, "df.jdf")

df_loaded = loadjdf("df.jdf")

@test df == df_loaded

ar_timezone = [ar.timezone for ar in ar]
meta = open("plsdel.io", "w") do io
  compress_then_write(ar_timezone, io)
end

ar_timezone_copy = open("plsdel.io") do io
  column_loader(meta.type, io, meta)
end

@test ar_timezone == ar_timezone_copy

meta = open("plsdel.io", "w") do io
  compress_then_write(a.timezone.transitions, io)
end

a_timezone_transitions_loaded = open("plsdel.io") do io
  column_loader(meta.type, io, meta)
end

@test a.timezone.transitions == a_timezone_transitions_loaded


rm("plsdel.io", force=true)
rm("df.jdf", force=true, recursive=true)

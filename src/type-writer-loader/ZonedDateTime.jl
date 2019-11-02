# ##############################################################################
# ZonedDateTime
# ##############################################################################
# the structure
  # utc_datetime : isbits
  # timezone : VariableTimeZone
  #   name : string
  #   transitions : Vector{TimeZones.Transitions}
  #     utc_datetime : isbits
  #     zone: FixedTimeZone
  #       name : string
  #       offset : isbits
  #   cutoff : isbits (Union{Nothing, DateTime})
  # zone
  #   name : string
  #   offset : isbits
compress_then_write(b::Vector{ZonedDateTime}, io) = begin
  utc_datetime_meta = compress_then_write([b.utc_datetime for b in b], io)
  timezone_meta = compress_then_write([b.timezone for b in b], io)
  zone_meta = compress_then_write([b.zone for b in b], io)

  (type = ZonedDateTime, len = utc_datetime_meta.len + timezone_meta.len + zone_meta.len, utc_datetime_meta = utc_datetime_meta, timezone_meta = timezone_meta, zone_meta = zone_meta)
end

# load a ZonedDateTime column
column_loader(::Type{ZonedDateTime}, io, metadata) = begin
  v1 = column_loader(metadata.utc_datetime_meta.type, io, metadata.utc_datetime_meta)
  v2 = column_loader(metadata.timezone_meta.type, io, metadata.timezone_meta)
  v3 = column_loader(metadata.zone_meta.type, io, metadata.zone_meta)
  [ZonedDateTime(v1, v2, v3) for (v1, v2, v3) in zip(v1, v2, v3)]
end

column_loader!(_, ::Type{ZonedDateTime}, io, metadata) = begin
    column_loader(ZonedDateTime, io, metadata)
end

some_elm(::Type{ZonedDateTime}) = ZonedDateTime(1984, 1, 5, tz"Asia/Shanghai")

# ##############################################################################
# TimeZones.Transitions
# ##############################################################################
compress_then_write(b::Vector{TimeZones.Transition}, io) = begin
  # the structure of Transition is like
  # (utc_datetime::isbits, zone::string)
  utc_datetime_meta = compress_then_write([b.utc_datetime for b in b], io)
  zone_meta = compress_then_write([b.zone for b in b], io)

  (type = TimeZones.Transition, len = utc_datetime_meta.len + zone_meta.len, utc_datetime_meta = utc_datetime_meta, zone_meta = zone_meta)
end

column_loader(::Type{TimeZones.Transition}, io, metadata) = begin
  v1 = column_loader(metadata.utc_datetime_meta.type, io, metadata.utc_datetime_meta)
  v2 = column_loader(metadata.zone_meta.type, io, metadata.zone_meta)
  [TimeZones.Transition(v1, v2) for (v1, v2) in zip(v1, v2)]
end

column_loader!(::Type{TimeZones.Transition}, io, metadata) = begin
    column_loader(TimeZones.Transition, io, metadata)
end

# ##############################################################################
# VariableTimeZone
# ##############################################################################
compress_then_write(b::Vector{VariableTimeZone}, io) = begin
  # the structure of Transition is like
  # (name::string, transitions, cutoff::string)
  name_meta = compress_then_write([b.name for b in b], io)

  len_transitions = [length(b.transitions) for b in b]
  len_transitions_meta = compress_then_write(len_transitions, io)

  transitions = Vector{TimeZones.Transition}(undef, sum(len_transitions))
  i = 1
  for b in b
    for t in b.transitions
      transitions[i] = t
      i += 1
    end
  end
  transitions_meta = compress_then_write(transitions, io)
  cutoff_meta = compress_then_write([b.cutoff for b in b], io)

  (
    type = VariableTimeZone,
    len = name_meta.len + len_transitions_meta.len + transitions_meta.len + cutoff_meta.len,
    name_meta = name_meta, len_transitions_meta = len_transitions_meta,
    transitions_meta = transitions_meta, cutoff_meta = cutoff_meta
  )
end

column_loader(::Type{VariableTimeZone}, io, metadata) = begin
  name = column_loader(metadata.name_meta.type, io, metadata.name_meta)

  len_transitions = column_loader(metadata.len_transitions_meta.type, io, metadata.len_transitions_meta)
  hi = cumsum(len_transitions)
  lo = vcat(0, hi[1:end-1]) .+ 1


  transitions = column_loader(metadata.transitions_meta.type, io, metadata.transitions_meta)

  metadata.cutoff_meta.type
  cutoff = column_loader(metadata.cutoff_meta.type, io, metadata.cutoff_meta)


  [VariableTimeZone(name, transitions[lo:hi], cutoff) for (name, lo, hi, cutoff) in zip(name, lo, hi, cutoff)]
end

column_loader!(::Type{VariableTimeZone}, io, metadata) = begin
    column_loader(VariableTimeZone, io, metadata)
end

# ##############################################################################
# FixedTimeZone
# ##############################################################################
compress_then_write(b::Vector{FixedTimeZone}, io) = begin
  # the structure of Transition is like
  # (name, offset)
  m1 = compress_then_write([b.name for b in b], io)
  m2 = compress_then_write([b.offset for b in b], io)

  (type = FixedTimeZone, len = m1.len + m2.len, name_meta = m1, offset_meta = m2)
end

column_loader(::Type{FixedTimeZone}, io, metadata) = begin
  v1 = column_loader(metadata.name_meta.type, io, metadata.name_meta)
  v2 = column_loader(metadata.offset_meta.type, io, metadata.offset_meta)
  [FixedTimeZone(v1, v2) for (v1, v2) in zip(v1, v2)]
end

column_loader!(::Type{FixedTimeZone}, io, metadata) = begin
    column_loader(FixedTimeZone, io, metadata)
end

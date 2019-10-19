using Revise
using JDF

a=gf()
b = Array(a.ORIGIN_STATE_ABR)
io = iow()
@which compress_then_write(eltype(b), b, io)
@time metadata = compress_then_write(eltype(b), b, io)
close(io)

io = ior()
@which column_loader(eltype(b), b, io)

@time okhl = column_loader(eltype(b), io, metadata)
close(io)


all(skipmissing(b) .== skipmissing(okhl))

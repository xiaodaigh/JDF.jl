using Revise

using JDF

using CSV, DataFrames, Blosc, JLSO, Base.GC

# use 12 threads
Blosc.set_num_threads(6)
# GC.gc()
@time a = CSV.read(
    "C:/data/Performance_All/Performance_2010Q3.txt",
    delim = '|',
    header = false
);

GC.gc()


t = time()
@time metadatas = psavejdf(a, "c:/data/large.dir.jdf");
time() - t
GC.gc()

GC.gc()
@time metadatas = savejdf(a, "c:/data/large.jdf");
GC.gc()

@time JLSO.save("c:/data/large.meta.jlso", metadatas)
GC.gc()

using Revise, JLSO, JDF, DataFrames
@time metadatas = JLSO.load("c:/data/large.meta.jlso")["data"];
GC.gc()

@time a2 = loadjdf("c:/data/large.jdf", metadatas);
GC.gc()

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

@time a = "id".*string.(rand(UInt16,100_000_000))
fn1() = begin
    io = open("c:/data/io.tmp", "w")
    res = write.(Ref(io), a);
    close(io)
    res
end


using BufferedStreams
fn2() = begin
    io = BufferedOutputStream(open("c:/data/io.tmp", "w"));
    res = write.(Ref(io), a);
    close(io)
    res
end

@time fn1(a);

using BenchmarkTools
@btime fn1(a);
@btime fn2();

using TranscodingStreams, CodecZlib

using TranscodingStreams, CodecZlib

fn3() = begin
    io = TranscodingStream(GzipCompressor(), open("c:/data/io_cmp.tmp", "w")) |>
            BufferedOutputStream

    res = write.(Ref(io), a);
    close(io)
    res
end

@btime fn3();

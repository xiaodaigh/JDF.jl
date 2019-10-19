using Revise

using JDF

using CSV, DataFrames, Blosc, JLSO, Base.GC

# use 12 threads
Blosc.set_num_threads(6)
# GC.gc()
@time a = CSV.read(
    "C:/data/Performance_All/Performance_2016Q1.txt",
    delim = '|',
    header = false
);

GC.gc()


t = time()
@time metadatas = psavejdf(a, "c:/data/large.jdf");
time() - t
GC.gc()

GC.gc()
@time metadatas = savejdf(a, "c:/data/large.jdf");
GC.gc()

@time JLSO.save("c:/data/large.meta.jlso", metadatas)
GC.gc()

using Revise, JLSO, JDF
@time metadatas = JLSO.load("c:/data/large.meta.jlso")["data"];
GC.gc()

@time a2 = loadjdf("c:/data/large.jdf", metadatas);
GC.gc()

all(names(a) .== names(a2))
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)]))

# CSVFiles vs CSV
using Revise
using CSV, DataFrames, JDF, Base.GC
import Base.Threads: @spawn

# a = gf()
fp = [f for f in readdir("c:/data/AirOnTimeCSV/") if endswith(f, ".csv")];

using CSVFiles

@time a= load(joinpath("c:/data/AirOnTimeCSV", fp[1]), type_detect_rows = 2000)

#psavejdf(a, joinpath("c:/data/hehe/", fp[1]))
sort!(fp, by = x->filesize(joinpath("c:/data/AirOnTimeCSV/", x)), rev=true)

fn4(fp) = begin
    #@time a = load(joinpath("C:/data/AirOnTimeCSV", fp), type_detect_rows = 2000) |> DataFrame
    @time a = CSV.read(joinpath("C:/data/AirOnTimeCSV", fp))
    #mkdir(joinpath("c:/data/hehe/", fp))
    res = psavejdf(a, joinpath("c:/data/hehe/", fp))
    a = nothing
    res
end

res = Vector{Any}(undef, length(fp))

@time for (i, f) in enumerate(fp[1:6])
    print(i)
    res[i] = fn4(f)
end

#@time fn4(fp[1])
#@time fn4.(fp)
2+2

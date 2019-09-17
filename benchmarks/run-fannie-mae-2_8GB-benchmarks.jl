# Fannie Mae

# download("http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2007.tgz", "c:/data/mortgage_2000-2007.tgz")

# un-tar and uncompress the file
# ;tar zxvg mortgage_2000-2007.tgz


# uncomment for debugging
dirpath = "C:/data/perf/"
largest_file = "Performance_2004Q3.txt"
outpath = "c:/data/jdf-bench/Performance_2004Q3.txt"
data_label =  "Fannie Mae Performance 2004Q3"
# delim = ','
# header = true

include("C:/Users/RTX2080/git/JDF/benchmarks/benchmarks.jl")
#@timegen_benchmark("c:/data/AirOnTimeCSV/", "airOT199302.csv", "c:/data/jdf-bench/airOT199302.csv", "Air On Time 199302")
@time res = gen_benchmark(dirpath, largest_file, outpath, data_label, delim = '|', header = false);

write_perf = res[1]
read_perf = res[2]
pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
run_group = repeat(["1st", "2nd"], outer = 6)

write_perf = write_perf[pkgs .!= "Feather.jl"]
run_group = run_group[pkgs .!= "Feather.jl"]
pkgs1 = pkgs[pkgs .!= "Feather.jl"]

plot_write = groupedbar(
    pkgs1,
    write_perf,
    group = run_group,
    ylab = "Seconds",
    title = "Disk-format Write performance comparison \n Julia $(VERSION)"
)
savefig(plot_write, joinpath(outpath, largest_file*"plot_write_less.png"))

pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
run_group = repeat(["1st", "2nd"], outer = 6)
read_perf = read_perf[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
run_group = run_group[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
pkgs2 = pkgs[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]

plot_read = groupedbar(
    pkgs2,
    read_perf,
    group = run_group,
    ylab = "Seconds",
    title = "Disk-format Read performance comparison \n Julia $(VERSION)"
)
savefig(plot_read, joinpath(outpath, largest_file*"plot_read_less.png"))


write_perf = res[1]
read_perf = res[2]
pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
run_group = repeat(["1st", "2nd"], outer = 6)

write_perf = write_perf[pkgs .!= "Feather.jl"]
run_group = run_group[pkgs .!= "Feather.jl"]
pkgs1 = pkgs[pkgs .!= "Feather.jl"]

plot_write = groupedbar(
    pkgs1,
    write_perf,
    group = run_group,
    ylab = "Seconds",
    title = "Disk-format Write performance comparison \n Julia $(VERSION)"
)
savefig(plot_write, joinpath(outpath, largest_file*"plot_write_less.png"))

pkgs = repeat(["JDF.jl", "CSV.jl", "Feather.jl", "data.table", "fst", "parquet R"], inner = 2)
run_group = repeat(["1st", "2nd"], outer = 6)
read_perf = read_perf[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
run_group = run_group[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]
pkgs2 = pkgs[.!in.(pkgs, Ref(["CSV.jl", "parqeut R"]))]

plot_read = groupedbar(
    pkgs2,
    read_perf,
    group = run_group,
    ylab = "Seconds",
    title = "Disk-format Read performance comparison \n Julia $(VERSION)"
)
savefig(plot_read, joinpath(outpath, largest_file*"plot_read_less.png"))

sizedf = sum_file_size(outpath)

using StatsPlots

p = plot(
    sizedf.pkg,
    sizedf.fs/1024^3,
    linetype = :bar,
    ylab = "Size (GB)",
    legend = false,
    title = "On-disk file Size for various formats\n $data_label data")
savefig(p, joinpath(outpath, largest_file*"_filesize.png"))

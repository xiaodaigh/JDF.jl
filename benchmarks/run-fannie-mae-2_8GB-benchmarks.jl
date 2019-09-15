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
@time gen_benchmark(dirpath, largest_file, outpath, data_label, delim = '|', header = false);


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

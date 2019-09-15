# Fannie Mae

# download("http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000.tgz", "c:/data/mortgage_2000.tgz")

# un-tar and uncompress the file
# ;tar zxvg c:/data/mortgage_2000.tgz


# uncomment for debugging
dirpath = "C:/data/Performance_All/"
largest_file = "Performance_2000Q4.txt"
outpath = "c:/data/jdf-bench/Performance_2000Q4.txt"
data_label =  "Fannie Mae Performance 2000Q4"
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

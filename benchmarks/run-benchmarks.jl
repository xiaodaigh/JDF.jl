# dirpath = "C:/data/Performance_All/"
# largest_file = "Performance_2000Q4.txt"
# outpath = "c:/data"
#

# download("https://packages.revolutionanalytics.com/datasets/AirOnTime87to12/AirOnTimeCSV.zip")
# ;unzip c:/data/AirOnTimeCSV AirOnTimeCSV.zip

# Uncomment for debugging
dirpath = "c:/data/AirOnTimeCSV/"
largest_file = "airOT199302.csv"
outpath = "c:/data/jdf-bench/airOT199302.csv"
data_label = "Air On Time 199302"
delim = ','
header = true

include("C:/Users/RTX2080/git/JDF/benchmarks/benchmarks.jl")
@time res = gen_benchmark("c:/data/AirOnTimeCSV/", "airOT199302.csv", "c:/data/jdf-bench/airOT199302.csv", data_label)
# gen_benchmark("C:/data/Performance_All/", "Performance_2000Q4.txt", "c:/data/jdf-bench/Performance_2000Q4.txt", "Fannie Mae Performance 2000Q4")


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

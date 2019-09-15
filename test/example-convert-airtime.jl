using Revise
using JDF, CSV, DataFrames
const path = "c:/data/AirOnTimeCSV"
const outpath = "c:/data/AirOnTimeCSV_jdf_pp"

convert_csv_to_jdf(infile, outdir) = begin
	print("$(infile): csv read: ")
	@time df = CSV.read(infile)
	print("$(infile): compression: ")
	@time type_compress!(df)
	print("$(infile): write_out: ")
	@time psavejdf(outdir, df)
end

for infile in readdir(path)
	@time convert_csv_to_jdf(
		joinpath(path, infile),
		joinpath(outpath, infile)
	)
	println("")
	println("")
end

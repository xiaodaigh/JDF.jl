using CSV
a = gf()
CSV.write("c:/data/a.csv", a)
;gzip c:/data/a.csv


using CodecZlib, BufferedStreams
io = open("c:/data/a.csv.gz") |> GzipDecompressorStream |> BufferedInputStream
@time a2 = CSV.read(io)
close(io)


using ZipFile, CSV, DataFrames, BufferedStreams

a = DataFrame(a = 1:3)
CSV.write("c:/data/a.csv", a)

# zip the file; Windows users who do not have zip available on the PATH can manual zip the CSV
;zip c:/data/a.zip c:/data/a.csv

io = open("c:/data/a.zip", "r")
z = ZipFile.Reader(io)

df = CSV.read(z.files[1])
close(io)

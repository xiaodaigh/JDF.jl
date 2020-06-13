
using RDatasets, JDF, DataFrames

a = dataset("datasets", "iris");

first(a, 2)


@time jdffile = savejdf("iris.jdf", a)
@time a2 = loadjdf("iris.jdf")


all(names(a2) .== names(a)) # true
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)])) #true


a2_selected = loadjdf("iris.jdf", cols = [:Species, :SepalLength, :PetalWidth])


jdf"path/to/JDF.jdf"


path_to_JDF = "path/to/JDF.jdf"
JDFFile(path_to_JDF)


afile = JDFFile("iris.jdf")

afile[!, :Species] # load Species column
afile[!, [:Species, :PetalLength]] # load Species and PetalLength column

afile[:, :Species] # load Species column
afile[:, [:Species, :PetalLength]] # load Species and PetalLength column

@view(afile[!, :Species]) # load Species column
@view(afile[!, [:Species, :PetalLength]]) # load Species and PetalLength column


using Tables
ajdf = JDFFile("iris.jdf")
Tables.columnaccess(ajdf)


Tables.columns(ajdf)


Tables.schema(ajdf)


getproperty(Tables.columns(ajdf), :Species)


jdffile = jdf"iris.jdf"
for col in eachcol(jdffile)
  # do something to col
  # where `col` is the content of one column of iris.jdf
end


jdffile = jdf"iris.jdf"
for (name, col) in zip(names(jdffile), eachcol(jdffile))
  # `name::Symbol` is the name of the column
  #  `col` is the content of one column of iris.jdf
end


using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
savejdf(df, "plsdel.jdf")


names(jdf"plsdel.jdf") # [:a, :b]

nrow(jdf"plsdel.jdf") # 3

ncol(jdf"plsdel.jdf") # 2

size(jdf"plsdel.jdf") # (3, 2)

size(jdf"plsdel.jdf", 1) # 2

size(jdf"plsdel.jdf", 2) # 3

# clean up
rm("plsdel.jdf", force = true, recursive = true)


@time jdffile = ssavejdf("iris.jdf", a)
@time jdffile = sloadjdf("iris.jdf")


type_compress!(df)


type_compress!(df, compress_float = true)


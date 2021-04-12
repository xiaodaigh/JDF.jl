using DataFrames
using Random: randstring
using JDF
using Test

df = DataFrame([collect(1:10) for i = 1:3000], :auto);
df[!, :strs] = [randstring(8) for i = 1:DataFrames.nrow(df)]
df[!, :substrs] = [SubString(x, 1, 3) for x in df[!, :strs]]


JDF.save("tmp-substring.jdf", df)

df2 = DataFrame(JDF.load("tmp-substring.jdf"))

@test df.substrs == df2.substrs

rm("tmp-substring.jdf", force=true, recursive=true)


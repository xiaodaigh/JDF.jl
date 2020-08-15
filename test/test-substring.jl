using DataFrames
using Random: randstring
using JDF
using Test

df = DataFrame([collect(1:100) for i = 1:3000]);
df[!, :strs] = [randstring(8) for i = 1:nrow(df)]
df[!, :substrs] = [SubString(x, 1, 3) for x in df[!, :strs]]

io = IOBuffer()

JDF.save("tmp-substring.jdf", df)

df2 = JDF.load("tmp-substring.jdf")

@test df.substrs == df2.substrs




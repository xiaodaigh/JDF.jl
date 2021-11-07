# Guard aginst github #62
using JDF
using Dates
col = [Date(1999,1,1), missing]
df = (d = col, a = [1, missing])

using Tables

JDF.save("plsdel-date-w-missing.jdf", df)
DataFrame(JDF.load("plsdel-date-w-missing.jdf"))
rm("plsdel-date-w-missing.jdf", recursive=true)

# Guard aginst github #72
using DataFrames
using Dates

df = DataFrame()
df[!, :test] = [Dates.DateTime(2000,1,1,1,1,1), missing]
df
using JDF
JDF.savejdf("plsdel-datetime-w-missing.jdf", df)
DataFrame(JDF.load("plsdel-datetime-w-missing.jdf"))
rm("plsdel-datetime-w-missing.jdf", recursive=true)
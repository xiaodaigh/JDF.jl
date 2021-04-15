# Guard aginst github #62
using JDF
using Dates
col = [Date(1999,1,1), missing]
df = (d = col, a = [1, missing])
JDF.save("plsdel-date-w-missing.jdf", df)
DataFrame(JDF.load("plsdel-date-w-missing.jdf"))
rm("plsdel-date-w-missing.jdf", recursive=true)

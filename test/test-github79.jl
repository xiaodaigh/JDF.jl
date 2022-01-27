using Test;
using JDF, DataFrames

a = DataFrame(a = 1:3, b =4:6)

tf = tempname()

JDF.save(tf, a)
a = DataFrame(JDF.load(tf))

a1 = DataFrame(JDF.load(tf; cols = ["b", "a"]))

@test a1.a == 1:3
@test a1.b == 4:6


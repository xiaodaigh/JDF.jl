# JDF
Let `a` be a `DataFrame`. You save it and load it using below. Next step is to
save the metadata as well.

```julia
@time metadatas = savejdf(a, "c:/data/a.jdf")
@time a2 = loadjdf("c:/data/a.jdf", metadatas)

all(names(a2) .== names(a)) # true
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)])) #true
```

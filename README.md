# JDF
An experimental DataFrames serialization format with the following goals
* Fast save and load times
* Compressed storage on disk

JDF stores a dataframe in a folder with each column stored as a separate file. There is also a `metadata.jls` file that stores metadata about the original dataframe. Collectively, the column files, the metadata file, and the folder is called a JDF "file".

## Example: Quick Start

```julia
using VegaDatasets, JDF, DataFrames

iris = dataset("iris") |> DataFrame
```

### *Saving* and *Loading* data
By default JDF loads and saves `DataFrame`s using parallel process starting from Julia 1.3. For Julia < 1.3, it saves and loads using one thread only.
```julia
@time metadatas = savejdf("c:/data/iris.jdf", a)
@time a2 = loadjdf("c:/data/iris.jdf")
```

Simple checks for correctness
```julia
all(names(a2) .== names(a)) # true
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)])) #true
```

### Save and load serially
You can use the `ssavejdf` and `sloadjdf` function to save a `DataFrame` using
serially, i.e. without using parallel processes.
```julia
@time metadatas = ssavejdf("c:/data/iris.jdf", a)
@time metadatas = sloadjdf("c:/data/iris.jdf")
```

### Additional functionality: In memory `DataFrame` compression
`DataFrame` sizes are out of control. A 2GB CSV file can easily take up 10GG in RAM. One can use the function `type_compress!(df)`  to compress any `df::DataFrame`. E.g.

```julia
type_compress!(df)
```

The function looks at `Int*` columns and see if it can be safely "downgraded" to another `Int*` type with a smaller bits size. It will convert `Float64` to `Float32` if `compress_float = true`. E.g.

```julia
type_compress!(df, compress_float = true)
```

`String` compression is _planned_ and will likely employ categorical encoding combined RLE encoding.

## Benchmarks
Here are some benchmarks [Fannie Mae Mortgage Data](https://docs.rapids.ai/datasets/mortgage-data). Please note that a reading of zero means that the method has failed to read or write.

JDF is a decent performaner on both read and write and can achieve comparable performance to [R's {fst}](https://www.fstpackage.org/), once compiled. The JDF format also results in much smaller file size vs Feather.jl in this particular example (probably Feather.jl's inefficient storage of `Union{String, Missing}`).

![](benchmarks/results/fannie-mae-read-Performance_2004Q3.txt.png)
![](benchmarks/results/fannie-mae-write-Performance_2004Q3.txt.png)
![](benchmarks/results/fannie-mae-filesize-Performance_2004Q3.txt.png)

Please note that the benchmarks are obtained on Julia 1.3+. On earlier version of Julia where multi-threading isn't available, JDF is roughly 2x slower than as shown in the benchmarks.

## Supported data types
There is support for `String`, `Bool`, and `isbits` types i.e. `UInt*`, `Int*`, and `Float*` `Date*` types etc.  Restricting the types that JDF support is vital for simplicity.

Further support will be added for `CategoricalVectors` and `RLEVectors` in the future.

## How does JDF work?
Although JDF is experimental, there a few tricks up Julia's sleeves. Firstly, this is a purely Julia solution and there a lot of ways to do nifty things like compression and encapsulating the underlying struture of the array that's hard to do in R and Python. E.g. Python's numpy arrays are C objects, but all the vector types used in JDF are Julia data.

When saving a JDF, each vector is Blosc compressed (using the default settings) if possible; this includes all `T` and `Unions{Missing, T}` types where `T` is `isbits`. For `String` vectors, they are first converted to Run Length Encoding (RLE), and the lengths component in the RLE are `Blosc` compressed.

## Notes
Parallel read and write support is only available from Julia 1.3.

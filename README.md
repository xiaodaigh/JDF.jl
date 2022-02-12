# What is JDF.jl?

JDF is a `DataFrame`s serialization format with the following goals
* Fast save and load times
* Compressed storage on disk
* Enable disk-based data manipulation (not yet achieved)
* Supports machine learning workloads, e.g. mini-batch, sampling (not yet achieved)

JDF.jl is the Julia pacakge for all things related to JDF.

JDF stores a `DataFrame` in a folder with each column stored as a separate file.
There is also a `metadata.jls` file that stores metadata about the original
`DataFrame`. Collectively, the column files, the metadata file, and the folder
is called a JDF "file".

`JDF.jl` is a pure-Julia solution and there are a lot of ways to do nifty things
like compression and encapsulating the underlying struture of the arrays that's
hard to do in R and Python. E.g. Python's numpy arrays are C objects, but all
the vector types used in JDF are Julia data types.

## Please note

The next major version of JDF will contain breaking changes. But don't worry I am fully committed to providing an automatic upgrade path. This means that you can safely use JDF.jl to save your data and not have to worry about the impending breaking change breaking all your JDF files.


## Example: Quick Start

```julia
using RDatasets, JDF, DataFrames

a = dataset("datasets", "iris");

first(a, 2)
```

```
2×5 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     Cat…
─────┼───────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  setosa
   2 │         4.9         3.0          1.4         0.2  setosa
```





### *Saving* and *Loading* data
By default JDF loads and saves `DataFrame`s using multiple threads starting from
Julia 1.3. For Julia < 1.3, it saves and loads using one thread only.

```julia
@time jdffile = JDF.save("iris.jdf", a)
@time a2 = DataFrame(JDF.load("iris.jdf"))
```

```
0.091923 seconds (157.33 k allocations: 9.226 MiB, 98.89% compilation tim
e)
  0.165332 seconds (197.31 k allocations: 11.476 MiB, 98.49% compilation ti
me)
150×5 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     Cat…
─────┼─────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  setosa
   2 │         4.9         3.0          1.4         0.2  setosa
   3 │         4.7         3.2          1.3         0.2  setosa
   4 │         4.6         3.1          1.5         0.2  setosa
   5 │         5.0         3.6          1.4         0.2  setosa
   6 │         5.4         3.9          1.7         0.4  setosa
   7 │         4.6         3.4          1.4         0.3  setosa
   8 │         5.0         3.4          1.5         0.2  setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮           ⋮
 144 │         6.8         3.2          5.9         2.3  virginica
 145 │         6.7         3.3          5.7         2.5  virginica
 146 │         6.7         3.0          5.2         2.3  virginica
 147 │         6.3         2.5          5.0         1.9  virginica
 148 │         6.5         3.0          5.2         2.0  virginica
 149 │         6.2         3.4          5.4         2.3  virginica
 150 │         5.9         3.0          5.1         1.8  virginica
                                                   135 rows omitted
```





Simple checks for correctness
```julia
all(names(a2) .== names(a)) # true
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)])) #true
```

```
true
```





### Loading only certain columns
You can load only a few columns from the dataset by specifying `cols =
[:column1, :column2]`. For example
```julia
a2_selected = DataFrame(JDF.load("iris.jdf", cols = [:Species, :SepalLength, :PetalWidth]))
```

```
150×3 DataFrame
 Row │ SepalLength  PetalWidth  Species
     │ Float64      Float64     Cat…
─────┼────────────────────────────────────
   1 │         5.1         0.2  setosa
   2 │         4.9         0.2  setosa
   3 │         4.7         0.2  setosa
   4 │         4.6         0.2  setosa
   5 │         5.0         0.2  setosa
   6 │         5.4         0.4  setosa
   7 │         4.6         0.3  setosa
   8 │         5.0         0.2  setosa
  ⋮  │      ⋮           ⋮           ⋮
 144 │         6.8         2.3  virginica
 145 │         6.7         2.5  virginica
 146 │         6.7         2.3  virginica
 147 │         6.3         1.9  virginica
 148 │         6.5         2.0  virginica
 149 │         6.2         2.3  virginica
 150 │         5.9         1.8  virginica
                          135 rows omitted
```




The difference with loading the whole datasets and then subsetting the columns
is that it saves time as only the selected columns are loaded from disk.

### Some `DataFrame`-like convenience syntax/functions
To take advatnage of some these convenience functions, you need to create a variable of type `JDFFile` pointed to the JDF file on disk. For example

```julia
jdf"path/to/JDF.jdf"
```

```
JDFFile{String}("path/to/JDF.jdf")
```




or
```julia
path_to_JDF = "path/to/JDF.jdf"
JDFFile(path_to_JDF)
```

```
JDFFile{String}("path/to/JDF.jdf")
```





#### Using `df[col::Symbol]` syntax
You can load arbitrary `col` using the `df[col]` syntax. However, some of these operations are not
yet optimized and hence may not be efficient.

```julia
afile = JDFFile("iris.jdf")

afile[:Species] # load Species column
```

```
150-element CategoricalArrays.CategoricalArray{String,1,UInt8}:
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 ⋮
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
```






#### JDFFile is Table.jl columm-accessible

```julia
using Tables
ajdf = JDFFile("iris.jdf")
Tables.columnaccess(ajdf)
```

```
true
```



```julia
Tables.columns(ajdf)
```

```
JDFFile{String}("iris.jdf")
```



```julia
Tables.schema(ajdf)
```

```
Tables.Schema:
 :SepalLength  Float64
 :SepalWidth   Float64
 :PetalLength  Float64
 :PetalWidth   Float64
 :Species      CategoricalVector (alias for CategoricalArrays.CategoricalAr
ray{T, 1} where T)
```



```julia
getproperty(Tables.columns(ajdf), :Species)
```

```
150-element CategoricalArrays.CategoricalArray{String,1,UInt8}:
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 "setosa"
 ⋮
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
 "virginica"
```






#### Load each column from disk
You can load each column of a JDF file from disk using iterations

```julia
jdffile = jdf"iris.jdf"
for col in eachcol(jdffile)
  # do something to col
  # where `col` is the content of one column of iris.jdf
end
```




To iterate through the columns names and the `col`

```julia
jdffile = jdf"iris.jdf"
for (name, col) in zip(names(jdffile), eachcol(jdffile))
  # `name::Symbol` is the name of the column
  #  `col` is the content of one column of iris.jdf
end
```




#### Metadata Names & Size from disk
You can obtain the column names and number of columns `ncol` of a JDF, for
example:


```julia
using JDF, DataFrames
df = DataFrame(a = 1:3, b = 1:3)
JDF.save(df, "plsdel.jdf")

names(jdf"plsdel.jdf") # [:a, :b]

# clean up
rm("plsdel.jdf", force = true, recursive = true)
```




### Additional functionality: In memory `DataFrame` compression
`DataFrame` sizes are out of control. A 2GB CSV file can easily take up 10GB in
RAM. One can use the function `type_compress!(df)`  to compress any
`df::DataFrame`. E.g.

```julia
type_compress!(df)
```

```
3×2 DataFrame
 Row │ a     b
     │ Int8  Int8
─────┼────────────
   1 │    1     1
   2 │    2     2
   3 │    3     3
```





The function looks at `Int*` columns and see if it can be safely "downgraded" to
another `Int*` type with a smaller bits size. It will convert `Float64` to
`Float32` if `compress_float = true`. E.g.

```julia
type_compress!(df, compress_float = true)
```

```
3×2 DataFrame
 Row │ a     b
     │ Int8  Int8
─────┼────────────
   1 │    1     1
   2 │    2     2
   3 │    3     3
```





`String` compression is _planned_ and will likely employ categorical encoding
combined with RLE encoding.



## Benchmarks
Here are some benchmarks using the [Fannie Mae Mortgage
Data](https://docs.rapids.ai/datasets/mortgage-data). Please note that a reading
of zero means that the method has failed to read or write.

JDF is a decent performer on both read and write and can achieve comparable
performance to [R's {fst}](https://www.fstpackage.org/), once compiled. The JDF
format also results in much smaller file size vs Feather.jl in this particular
example (probably due to Feather.jl's inefficient storage of `Union{String,
Missing}`).

![](benchmarks/results/fannie-mae-read-Performance_2004Q3.txt.png)
![](benchmarks/results/fannie-mae-write-Performance_2004Q3.txt.png)
![](benchmarks/results/fannie-mae-filesize-Performance_2004Q3.txt.png)

Please note that the benchmarks were obtained on Julia 1.3+. On earlier versions
of Julia where multi-threading isn't available, JDF is roughly 2x slower than as
shown in the benchmarks.

## Supported data types
I believe that restricting the types that JDF supports is vital for simplicity and maintainability.

There is support for
* `WeakRefStrings.StringVector`
* `Vector{T}`, `Vector{Union{Mising, T}}`, `Vector{Union{Nothing, T}}`
* `CategoricalArrays.CategoricalVetors{T}` and `PooledArrays.PooledVector`

where `T` can be `String`, `Bool`, `Symbol`, `Char`, `SubString{String}`, `TimeZones.ZonedDateTime` (experimental) and `isbits` types i.e. `UInt*`, `Int*`,
and `Float*` `Date*` types etc.

`RLEVectors` support will be considered in the future when `missing` support
arrives for `RLEVectors.jl`.

## Resources

[@bkamins](https://github.com/bkamins/)'s excellent [DataFrames.jl tutorial](https://github.com/bkamins/Julia-DataFrames-Tutorial/blob/master/04_loadsave.ipynb) contains a section on using JDF.jl.


## How does JDF work?
When saving a JDF, each vector is Blosc compressed (using the default settings)
if possible; this includes all `T` and `Unions{Missing, T}` types where `T` is
`isbits`. For `String` vectors, they are first converted to a  Run Length
Encoding (RLE) representation, and the lengths component in the RLE are `Blosc`
compressed.

## Development Plans
I fully intend to develop JDF.jl into a language neutral format by version v0.4. However, I have other OSS commitments including [R's
{disk.frame}](http:/diskframe.com) and hence new features might be slow to come onboard. But I am fully committed to making JDF files created using JDF.jl v0.2 or higher loadable in all future JDF.jl versions.

## Notes

* Parallel read and write support is only available from Julia 1.3.
* The design of JDF was inspired by [fst](https://www.fstpackage.org/) in terms of using compressions and allowing random-access to columns

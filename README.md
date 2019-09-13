# JDF
An experimental DataFrames serialization format with the following goals
* Fast save and load times
* Compressed storage on disk

JDF stores a dataframe in a folder with each column stored as a separate file. There is also a `metadata.jls` file that stores metadata about the original dataframe. Collectively, the column files, the metadata file, and the folder is called a JDF "file".

## Example: Quick Start

```julia
using VegaDatasets, JDF, DataFrames

a = dataset("iris") |> DataFrame
```

*Saving* and *Loading* data
```julia
@time metadatas = savejdf("c:/data/a.jdf", a)
@time a2 = loadjdf("c:/data/a.jdf")
```

Simple checks for correctness
```julia
all(names(a2) .== names(a)) # true
all(skipmissing([all(a2[!,name] .== Array(a[!,name])) for name in names(a2)])) #true
```

### Save in parallel
You can use the `psavejdf` function to save a `DataFrame` using parallel processes.
```julia
@time metadatas = psavejdf("c:/data/a.jdf", a)
```

## Supported data types
There is support for `String`, `Bool`, and `isbits` types i.e. `UInt*`, `Int*`, and `Float*` `Date*` types etc.  Restricting the types that JDF support is vital for simplicity.

## How does JDF work?
Although JDF is experimental, there a few tricks up Julia's sleeves. Firstly, this is a purely Julia solution and there a lot of ways to do nifty things like compression and encapsulating the underlying struture of the array that's hard to do in R and Python. E.g. Python's numpy arrays are C objects, but all the vector types used in JDF are Julia data.

When saving a JDF, each vector is Blosc compressed (using the default settings) if possible; this includes all `T` and `Unions{Missing, T}` types where `T` is `isbits`. For `String` vectors, they are first converted to Run Length Encoding (RLE), and the lengths component in the RLE are `Blosc` compressed.

## Notes
JDF only supports Julia 1.3 as Julia 1.1 and 1.2 seem to have bugs that prevent some operations in the JDF code.
Parallel support is planned, but currently only works for saving.

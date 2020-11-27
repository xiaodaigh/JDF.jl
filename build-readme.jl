# Weave readme
using Pkg
cd("c:/git/JDF/readme-build")
Pkg.activate("c:/git/JDF/readme-build")
Pkg.update()
upcheck()

using Weave

weave("../README.jmd", out_path="c:/git/JDF/", doctype="github")

if false
    # debug
    tangle("../README.jmd")
end

using ThreadPools: @bthreads
using DataFrames
using SortingLab: sorttwo!

mutable struct InProgressVector{T}  <: AbstractVector{T}
    v::Vector{T}
    isready::ReentrantLock
    parent::DataFrame
    name::String
end

Base.length(v::InProgressVector) = length(v.v)
Base.size(v::InProgressVector) = size(v.v)
Base.getindex(v::InProgressVector, args...) = begin
    lock(v.isready)
    vv = getindex(v.v, args...)
    unlock(v.isready)
    vv
end


# function barrier for assigining values
function assignreplace!(v::InProgressVector, idx)
    # to be taken
    lock(v.isready)
    v.v .= v.v[idx]
    v.parent[!, v.name] = v.v
    unlock(v.isready)
end

function fsort!(dataframe, by)
    idx = collect(1:nrow(dataframe))
    sorttwo!(dataframe[!, by], idx)

    all_except_by = setdiff(names(dataframe), [by])
    for n in all_except_by
        inprogressv = InProgressVector(dataframe[!, n], ReentrantLock(), dataframe, n)
        dataframe[!, n] = inprogressv
    end

    return dataframe

    @bthreads for n in all_except_by
        assignreplace!(dataframe[!, n], idx)
    end

    dataframe
end

@time df = DataFrame([rand(Int, 1_000_000) for i = 1:30]);
@time df1 = sort!(copy(df), "x1")
@time df2 = fsort!(copy(df), "x1") # takes 1 seconds

df2.x1

df1 == df2


[v1==v2 for (v1, v2) in zip(eachcol(df1), eachcol(df2))]

# faster sort of dataframe
function fsort!(dataframe, by)
    _, idx = sorttwo!(dataframe[!, by], collect(1:nrow(dataframe)))

    all_except_by = setdiff(names(dataframe), [by])
     @bthreads for n in all_except_by
        assignreplace!(dataframe[!, n], idx)
    end
    dataframe
end


# generate a dataframe with 300 columns
@time df = DataFrame([rand(Int, 1_000_000) for i = 1:300])
sort_time = @elapsed sort!(df, "x1") # takes 14 seconds

@time df = DataFrame([rand(Int, 1_000_000) for i = 1:300])
df = fsort2!(df, "x1") # takes 1 seconds

fsort2_time = @elapsed fsort2!(df, "x1") # takes 1 seconds


# sorting
@time df = DataFrame([rand(Int, 1_000_000) for i = 1:300])
fsort_time = @elapsed fsort!(df, "x1") # takes 1 seconds

using Plots
plot(
    ["DataFrames.jl sort", "New Sort", "New Sort with SortingLab.jl"],
    [sort_time, fsort2_time, fsort_time]; title = "Sorting DataFrames", seriestype=:bar)

savefig("perf.png")


function abc()
    cc = []
    for i in 1:3
        c = Channel{Bool}(1)
        push!(cc, c)
        put!(c, true)
        fetch(c) # wait for it
    end

    for i in 1:3
        println(i)
        take!(cc[i])
    end
end

@time abc()


using DataFrames

mutable struct IsReadyVector{T} <: AbstractVector{T}
    v::AbstractVector{T}
    isready::Channel{Bool}
end

Base.size(v::IsReadyVector) = size(v.v)

function def(n)
    df = DataFrame([rand(Int, 1_000_000) for i = 1:n])

    cc = []
    for i in names(df)
        newv = IsReadyVector(df[!, i], Channel{Bool}(1))
        df[!, i] = newv
        put!(newv.isready, true)
        fetch(newv.isready) # wait for it
    end

    for i in names(df)
        println(i)
        take!(df[!, i].isready)
    end
end

@time def(30)


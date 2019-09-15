

using CSV, Feather, JLD2, JLSO, JDF, FileIO, Blosc, StatsPlots
using DataFrames, WeakRefStrings # required for JLD2, JDF
Blosc.set_num_threads(6)

gen_benchmark(dirpath, largest_file, outpath, data_label; delim = ',', header=true) = begin
    if !isdir(outpath)
        mkpath(outpath)
    end

    csv_read1 = @elapsed df = CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header);
    csv_read2 = @elapsed      CSV.read(joinpath(dirpath, largest_file), delim = delim, header = header);

    csv_write1 = 0
    csv_write2 = 0
    try
        csv_write1 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
        csv_write2 = @elapsed CSV.write(joinpath(outpath, largest_file*".csv"), df);
    catch err
    end

    jlso_write1 = 0
    jlso_write2 = 0
    try
        jlso_write1 = @elapsed JLSO.save(joinpath(outpath, largest_file*".jlso"), df);
        jlso_write2 = @elapsed JLSO.save(joinpath(outpath, largest_file*".jlso"), df);
    catch err
    end

    jld2_write1 = 0
    jld2_write2 = 0
    try
        jld2_write1 = @elapsed save(joinpath(outpath, largest_file*".jld2"), Dict("df" => df));
        jld2_write2 = @elapsed save(joinpath(outpath, largest_file*".jld2"), Dict("df" => df));
    catch err
    end

    jdf_write1 = 0
    jdf_write2 = 0
    try
        jdf_write1 = @elapsed savejdf(joinpath(outpath, largest_file*".jdf"), df);
        jdf_write2 = @elapsed savejdf(joinpath(outpath, largest_file*".jdf"), df);
    catch err

    end

    # # feather can't handle all missing
    for n in names(df)
        if eltype(df[!,n]) == Missing
            println("Removed $n for Feather.jl")
            select!(df, Not(n))
            df[!,n] = Vector{Union{Missing, Bool}}(missing, size(df, 1))
        end
    end

    feather_write1 = 0
    feather_write2 = 0
    try
        feather_write1 = @elapsed Feather.write(joinpath(outpath, largest_file*".feather"), df);
        feather_write2 = @elapsed Feather.write(joinpath(outpath, largest_file*".feather"), df);
    catch err
    end


    #########################################  loading
    feather_read1 = 0
    feather_read2 = 0
    try
        feather_read1 = @elapsed Feather.read(joinpath(outpath, largest_file*".feather"));
        feather_read2 = @elapsed Feather.read(joinpath(outpath, largest_file*".feather"));
    catch err
    end

    jld2_read1 = 0
    jld2_read2 = 0
    try
        jld2_read1 = @elapsed load(joinpath(outpath, largest_file*".jld2"))["df"];
        jld2_read2 = @elapsed load(joinpath(outpath, largest_file*".jld2"))["df"];
    catch err
    end

    jlso_read1 = 0
    jlso_read2 = 0
    try
        jlso_read1 = @elapsed JLSO.load(joinpath(outpath, largest_file*".jlso"))["data"];
        jlso_read2 = @elapsed JLSO.load(joinpath(outpath, largest_file*".jlso"))["data"];
    catch err
    end

    jdf_read1 = 0
    jdf_read2 = 0
    try
        jdf_read1 = @elapsed loadjdf(joinpath(outpath, largest_file*".jdf"));
        jdf_read2 = @elapsed loadjdf(joinpath(outpath, largest_file*".jdf"));
    catch err
    end

    write_perf = [jdf_write1, jdf_write2, csv_write1, csv_write2, feather_write1, feather_write2, jld2_write1, jld2_write2, jlso_write1, jlso_write2]
    read_perf = [jdf_read1, jdf_read2, csv_read1, csv_read2, feather_read1, feather_read2, jld2_read1, jld2_read2, jlso_read1, jlso_read2]

    plot_write = groupedbar(
        repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl", "JLSO.jl"], inner = 2),
        write_perf,
        group = repeat(["1st", "2nd"], outer = 5),
        ylab = "Seconds",
        title = "Disk-format Write performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_write, joinpath(outpath, largest_file*"plot_write.png"))

    plot_write_wo_jlso = groupedbar(
        repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl"], inner = 2),
        [jdf_write1, jdf_write2, csv_write1, csv_write2, feather_write1, feather_write2, jld2_write1, jld2_write2],
        group = repeat(["1st", "2nd"], outer = 4),
        ylab = "Seconds",
        title = "Disk-format Write performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_write_wo_jlso, joinpath(outpath, largest_file*"plot_write_wo_jlso.png"))

    plot_read = groupedbar(
        repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl", "JLSO.jl"], inner = 2),
        read_perf,
        group = repeat(["1st", "2nd"], outer = 5),
        ylab = "Seconds",
        title = "Disk-format Read performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_read, joinpath(outpath, largest_file*"plot_read.png"))

    plot_read_wo_jlso = groupedbar(
        repeat(["JDF.jl", "CSV.jl", "Feather.jl", "JLD2.jl"], inner = 2),
        [jdf_read1, jdf_read2, csv_read1, csv_read2, feather_read1, feather_read2, jld2_read1, jld2_read2],
        group = repeat(["1st", "2nd"], outer = 4),
        ylab = "Seconds",
        title = "Disk-format Read performance comparison \n Data: $data_label data \n Size: $(size(df)) filesize:$(round(filesize(joinpath(dirpath, largest_file))/1024^3, digits=1))GB \n Julia $(VERSION)"
    )
    savefig(plot_read_wo_jlso, joinpath(outpath, largest_file*"plot_read_wo_jlso.png"))

    (write_perf, read_perf, dirpath, outpath, largest_file, df)
end


sum_file_size(dir) = begin
    res = Int[]
    for f in joinpath.(dir, readdir(dir))
        if isfile(f)
            push!(res, filesize(f))
        elseif isdir(f)
            println(f)
            push!(res, sum(filesize.(joinpath.(f,readdir(f)))))
        end
    end
    df = DataFrame(file = readdir(dir), fs = res)

    tmpfn = (x->x[end-3:end])
    df[!,:ext] = tmpfn.(df[!, :file])
    filter!(r -> r.ext != ".png", df)
    sort!(df, :ext)
    df[:pkg] = ["CSV.jl", "JDF.jl", "JLD2.jl", "JLSO.jl", "Feather.jl"]
    df
end

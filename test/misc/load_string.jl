using Revise

using JDF

using CSV, DataFrames, Blosc, JLSO, Base.GC

# use 12 threads
Blosc.set_num_threads(6)

@time a = CSV.read("C:/data/Performance_All/Performance_2010Q3.txt", delim = '|', header = false);


strs = "id".*string.(rand(UInt16, 100_000_000));
# write randomstring to io

strs = coalesce.(Array(a[:Column3]), "")
io = open("c:/data/string.jdf", "w")
@time string_byte_sizes = write.(Ref(io), strs);
close(io)

load_str(file, string_byte_sizes) = begin
    io = open(file, "r")
    tot_strings_in_bytes = sum(string_byte_sizes)
    strings_in_bytes = Vector{UInt8}(undef, tot_strings_in_bytes)
    @time read!(io, strings_in_bytes)
    close(io)

    i = 1
    j = 0
    ptr_to_string_in_bytes = pointer(strings_in_bytes)
    @time reconstituted_strings = String[" "^s for s in string_byte_sizes]
    @time for string_byte_size in string_byte_sizes
        #global i, j, reconstituted_strings
        #reconstituted_strings[i] = unsafe_string(ptr_to_string_in_bytes+j, string_byte_size)
        unsafe_copyto!(
            reconstituted_strings[i] |> pointer,
            ptr_to_string_in_bytes + j,
            string_byte_size)
        i += 1
        j += string_byte_size
    end
    reconstituted_strings
end

@time reconstituted_strings = load_str("c:/data/string.jdf", string_byte_sizes);

all(reconstituted_strings .== strs) # true: works but FAST


@time metadata = compress_then_write(a, "c:/data/string.jdf");

x = "abc"
xp = pointer(x)

unsafe_load(xp-8)


unsafe_pointer_to_objref(pointer(UInt8[3, 0, 0, 0, 0, 0, 0, 0, 63, 63, 63]))


unsafe_pointer_to_objref(xp-8)

y = unsafe_string(xp)
yp = pointer(y)
yp == xp # false

reinterpret(8, UInt8)

pointer(8)

unsafe_pointer_to_objref(pointer(UInt8[0,0,0,0, 0,0,0,2, 64, 65]))

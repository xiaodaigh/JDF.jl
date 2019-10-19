
unsafe_load(pointer.(strs) + 3)

a = "def"
b  = unsafe_string(pointer(a))

pointer(a)
pointer(b)

a = UInt8[64, 65, C_NULL, 62, 53]
pp= pointer(a)
print(pp)
@time as = String(a)
pointer(as)


unsafe_wrap(a, Ptr{String})

a = "def"
unsafe_load(pointer(a)+3)


x = "id".*string.(rand(UInt8, 1_000_000))

io = open("c:/data/io.bin", "w")
ncw = write.(Ref(io), x)
close(io)

io = open("c:/data/io.bin", "r")
buffer = Vector{UInt8}(undef, sum(ncw))
readbytes!(io, buffer, sum(ncw))
close(io)

@time aa = test(buffer)


function test(array)
    start = 1
    strings = String[]
    GC.@preserve array begin
        ptr = pointer(array) - 1
        for i in eachindex(array)
            @inbounds char = array[i]
            if char == UInt8(',')
                len = i - start
                str = unsafe_string(ptr + start, len)
                push!(strings, str)
                start = i + 1
            end
        end
    end
    strings
end

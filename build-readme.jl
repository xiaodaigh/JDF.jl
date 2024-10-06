# Weave readme
using Pkg
cd("c:/git/JDF/readme-build")
Pkg.activate("c:/git/JDF/readme-build")
Pkg.update()
upcheck()

using Weave

weave("../README.jmd", out_path="./", doctype="github")

if false
    # debug
    tangle("../README.jmd")
end

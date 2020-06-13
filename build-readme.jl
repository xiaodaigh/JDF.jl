# Weave readme
using Pkg
cd("c:/git/JDF")
Pkg.activate("c:/git/JDF")
Pkg.add("Weave")
Pkg.add("RDatasets")
Pkg.update()
using Weave

weave("README.jmd", out_path=:pwd, doctype="github")

tangle("README.jmd")
Pkg.rm("Weave")
Pkg.rm("RDatasets")

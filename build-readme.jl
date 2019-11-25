# Weave readme
using Pkg
Pkg.activate("c:/git/JDF")
using Weave

weave("README.jmd", out_path=:pwd, doctype="github")

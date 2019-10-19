using Documenter
using JDF

makedocs(
    sitename = "JDF",
    format = Documenter.HTML(),
    modules = [JDF]
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#

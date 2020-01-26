# ParallelOperations.jl

*Basic parallel algorithms for Julia*

[![codecov](https://codecov.io/gh/JuliaAstroSim/ParallelOperations.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaAstroSim/ParallelOperations.jl)

## Install

```julia
]add ParallelOperations
```
or
```julia
]add https://github.com/JuliaAstroSim/ParallelOperations.jl
```

## Usage

```julia
using Test
using Distributed
addprocs(4)

@everywhere using ParallelOperations

#!!! Notice
# User struct
@everywhere procs() struct TestStruct
    x
    y
end

# Define iterater methods to use REDUCE operations
@everywhere iterate(p::TestStruct) = (p, nothing)
@everywhere iterate(p::TestStruct, st) = nothing

# Functions to executed on remote workers should be defined on source worker
function f!(a::Array)
    for i in eachindex(a)
        a[i] = sin(a[i])
    end
end

# point-to-point
## Define a variable on worker and get back
sendto(2, a = 1.0)
b = getfrom(2, :a)
@test b == 1.0

## Specify module (optional)
#!!! Default module is Main
sendto(2, a = 1.0, ParallelOperations)
b = getfrom(2, :a, ParallelOperations)

## Get & Set data by Expr
@everywhere 2 s = TestStruct(0.0, 0.0)
b = 123.0

sendto(2, :(s.x), b)

sendto(2, :(s.y), 456.0)
@everywhere 2 @show s

## Transfer data from worker 2 to worker 3, and change symbol name
transfer(2, 3, :a, :b)
@everywhere 3 @show b


# broadcast & gather
bcast(workers(), :c, 1.0, Parallel)

bcast(workers(), c = [pi/2])

bcast(workers(), f!, :c)
d = gather(workers(), :(c[1]))
@test d == 4.0

# reduce
@everywhere workers() teststruct = TestStruct(myid(), collect(1:5) .+ myid())
M = reduce(max, workers(), :(teststruct.b))
```

## Similar packages

[ParallelDataTransfer](https://github.com/ChrisRackauckas/ParallelDataTransfer.jl)
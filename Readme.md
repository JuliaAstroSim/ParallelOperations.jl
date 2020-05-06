# ParallelOperations.jl

*Basic parallel algorithms for Julia*

[![codecov](https://codecov.io/gh/JuliaAstroSim/ParallelOperations.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaAstroSim/ParallelOperations.jl)

Features:
- User-friendly interface
- 100% auto-test coverage
- All of the operations could be executed on **specified Modules**
- Commonly used operations

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

# Functions to execute on remote workers should be known by target worker
@everywhere function f!(a::Array)
    for i in eachindex(a)
        a[i] = sin(a[i])
    end
end
```
### Point-to-point

```julia
## Define a variable on worker and get it back
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
```

Notice that functions would evaluate the parameter before sending them to remote workers. That means:
```julia
sendto(2, a = myid())
b = getfrom(2, :a)
```
would return `b = 1` instead of `2`, because function `myid` is executed on master process.

To send commands to remote, use macros:
```julia
@sendto 2 a = myid()
b = getfrom(2, :a)
# b = 2
```
Here `myid` is executed on process 2.

This also works with `bcast` and `@bcast` (in fact `@bcast` and `@sendto` have identical codes)

### broadcast

```julia
bcast(workers(), :c, 1.0, Parallel)

bcast(workers(), c = [pi/2])

bcast(workers(), f!, :c)
```

### gather

Gathering is executed in the order of the first parameter

```julia
d = gather(workers(), :(c[1]))
@test d == 4.0


bcast(pids, a = 1.0)
allgather(pids, :a, :b) # allgather data to new symbol (option)
                        # If ok with unstable type, you could use `allgather(pids, :a)`
b = gather(pids, :b)
@test sum(sum(b)) == 16.0
```

### reduce

```julia
@everywhere workers() teststruct = TestStruct(myid(), collect(1:5) .+ myid())
M = reduce(max, workers(), :(teststruct.b))


@everywhere pids a = myid()
allreduce(max, pids, :a) # allreduce data. Use allreduce(max, pids, :a, :b) for new symbol :b
b = gather(pids, :a)
@test sum(b) == 20.0
```

### Scatter

The array to scatter should have the same length as workers to receive

```julia
a = collect(1:4)
scatter(workers(), a, :b, Main)
@everywhere workers() @show b
```

### Commonly used functions

```julia
@everywhere workers() x = 1.0

sum(workers(), :x)
allsum(workers(), :x)
maximum(workers(), :x)
allmaximum(workers(), :x)
minimum(workers(), :x)
allminimum(workers(), :x)
```

## TODO

- [ ] Check remotecall functions
- [ ] Benchmark and optimization

## Similar packages

[ParallelDataTransfer](https://github.com/ChrisRackauckas/ParallelDataTransfer.jl)
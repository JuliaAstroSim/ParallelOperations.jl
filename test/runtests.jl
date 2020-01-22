using Test
using Distributed

using ParallelOperations

addprocs(4)
@everywhere workers() using ParallelOperations
@everywhere struct TestStruct
    a
    b
end
@everywhere iterate(p::TestStruct) = (p, nothing)
@everywhere iterate(p::TestStruct, st) = nothing

@everywhere workers() teststruct = TestStruct(myid(), collect(1:5) .+ myid())

@everywhere workers() x = myid()

@testset "Reduce" begin
    ReduceExpr = reduce(max, workers(), :(teststruct.b))
    @test ReduceExpr == 10

    ReduceSymbol = reduce(max, workers(), :x)
    @test ReduceSymbol == 5
end

@testset "Gather" begin
    GatherExpr = gather(workers(), :(teststruct.a))
    @test sum(GatherExpr) == sum(workers())

    GatherSymbol = gather(workers(), :x)
    @test sum(GatherSymbol) == sum(workers())
end

rmprocs(workers())
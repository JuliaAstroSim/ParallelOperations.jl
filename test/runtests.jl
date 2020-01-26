using Test
using Distributed

using ParallelOperations

pids = addprocs(4)
@everywhere pids using ParallelOperations
@everywhere struct TestStruct
    a
    b
end
@everywhere iterate(p::TestStruct) = (p, nothing)
@everywhere iterate(p::TestStruct, st) = nothing

@everywhere procs() function f(a::Array)
    for i in eachindex(a)
        a[i] = sin(a[i])
    end
end

@everywhere pids teststruct = TestStruct(myid(), collect(1:5) .+ myid())

@everywhere pids x = myid()

@testset "point-to-point" begin
    a = collect(1:3)
    sendto(pids[1], :p2p, a)
    b = getfrom(pids[1], :p2p)
    @test sum(a) == sum(b)

    sendto(pids[2], a = 1.0)
    b = getfrom(pids[2], :a)
    @test b == 1.0

    sendto(pids[3], a = [pi/2])
    sendto(pids[3], f, :a)
    b = getfrom(pids[3], :(a[1]))
    @test b == 1.0

    transfer(pids[3], pids[4], :a, :a)
    b = getfrom(pids[4], :(a[1]))
    @test b == 1.0
end

@testset "broadcast" begin
    c = 1.0
    bcast(pids, :c, c)
    d = gather(pids, :c)
    @test sum(d) == 4.0

    bcast(pids, c = myid())
    d = gather(pids, :c)
    @test sum(d) == 4.0

    bcast(pids, c = [pi/2])
    bcast(pids, f, :c)
    d = gather(pids, :(c[1]))
    @test sum(d) == 4.0
end

@testset "scatter" begin
    a = collect(1:4)
    scatter(pids, a, :b, Main)
    b = gather(pids, :b, Main)
    @test sum(a) == sum(b)

    @test_throws ErrorException scatter(pids, collect(1:5), :b, Main)
end

@testset "Reduce" begin
    ReduceExpr = reduce(max, pids, :(teststruct.b))
    @test ReduceExpr == 10

    ReduceSymbol = reduce(max, pids, :x)
    @test ReduceSymbol == 5
end

@testset "Gather" begin
    GatherExpr = gather(pids, :(teststruct.a))
    @test sum(GatherExpr) == sum(pids)

    GatherSymbol = gather(pids, :x)
    @test sum(GatherSymbol) == sum(pids)

    bcast(pids, c = pi / 2)
    d = gather(pids, sin, :c)
    @test sum(d) == 4.0
end

@testset "Allgather" begin
    bcast(pids, a = 1.0)
    allgather(pids, :a)
    b = gather(pids, :a)
    @test sum(sum(b)) == 16.0

    bcast(pids, a = 1.0)
    allgather(pids, :a, :b)
    b = gather(pids, :b)
    @test sum(sum(b)) == 16.0
end

@testset "Allreduce" begin
    @everywhere pids a = myid()
    allreduce(max, pids, :a)
    b = gather(pids, :a)
    @test sum(b) == 20.0

    @everywhere pids a = myid()
    allreduce(max, pids, :a, :b)
    b = gather(pids, :b)
    @test sum(b) == 20.0
end

rmprocs(pids)
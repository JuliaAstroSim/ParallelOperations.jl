using Distributed
using ParallelOperations
using BenchmarkTools

for np in [2, 4, 8]
    println("\n=== Testing with $np processes ===")
    
    pids = addprocs(np)
    @everywhere using ParallelOperations
    
    test_data = rand(1000, 1000)
    
    println("\n1. Testing sendto sync vs async performance:")
    println("   Sync version:")
    sync_result = @benchmark begin
        for p in $pids
            sendto(p, :test_data, $test_data)
        end
    end samples=3 evals=1
    println("   Sync time: $(round(mean(sync_result.times) / 1e9, digits=4)) seconds")
    
    println("   Async version:")
    async_result = @benchmark begin
        futures = []
        for p in $pids
            future = sendto_async(p, :test_data, $test_data)
            push!(futures, future)
        end
        fetch.(futures)
    end samples=3 evals=1
    println("   Async time: $(round(mean(async_result.times) / 1e9, digits=4)) seconds")
    println("   Speedup: $(round(mean(sync_result.times) / mean(async_result.times), digits=2))x faster")
    
    println("\n2. Testing bcast sync vs async performance:")
    println("   Sync version:")
    sync_result = @benchmark bcast($pids, :bcast_data, $test_data) samples=3 evals=1
    println("   Sync time: $(round(mean(sync_result.times) / 1e9, digits=4)) seconds")
    
    println("   Async version:")
    async_result = @benchmark begin
        futures = bcast_async($pids, :bcast_data, $test_data)
        fetch.(futures)
    end samples=3 evals=1
    println("   Async time: $(round(mean(async_result.times) / 1e9, digits=4)) seconds")
    println("   Speedup: $(round(mean(sync_result.times) / mean(async_result.times), digits=2))x faster")
    
    println("\n3. Testing gather sync vs async performance:")
    for p in pids
        sendto(p, :test_data, test_data)
    end
    
    println("   Sync version:")
    sync_result = @benchmark gather($pids, :test_data) samples=3 evals=1
    println("   Sync time: $(round(mean(sync_result.times) / 1e9, digits=4)) seconds")
    
    println("   Async version:")
    async_result = @benchmark begin
        futures = gather_async($pids, :test_data)
        fetch.(futures)
    end samples=3 evals=1
    println("   Async time: $(round(mean(async_result.times) / 1e9, digits=4)) seconds")
    println("   Speedup: $(round(mean(sync_result.times) / mean(async_result.times), digits=2))x faster")
    
    println("\n4. Testing scatterto sync vs async performance:")
    scatter_data = [rand(500, 500) for _ in 1:np]
    println("   Sync version:")
    sync_result = @benchmark scatterto($pids, $scatter_data, :scatter_data) samples=3 evals=1
    println("   Sync time: $(round(mean(sync_result.times) / 1e9, digits=4)) seconds")
    
    println("   Async version:")
    async_result = @benchmark begin
        futures = scatterto_async($pids, $scatter_data, :scatter_data)
        fetch.(futures)
    end samples=3 evals=1
    println("   Async time: $(round(mean(async_result.times) / 1e9, digits=4)) seconds")
    println("   Speedup: $(round(mean(sync_result.times) / mean(async_result.times), digits=2))x faster")
    
    rmprocs(pids)
end

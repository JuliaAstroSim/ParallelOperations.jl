module ParallelOperations

using Distributed

import Base: reduce

function fetch_with_timeout(future::Future, timeout::Float64 = 5.0)
    # Julia 1.12.4, wait do not support timeout
    return fetch(future)
end

export
    sendto, @sendto, sendto_async,
    getfrom, @getfrom, getfrom_async,
    transfer,

    bcast, @bcast, bcast_async,
    reduce,
    gather, @gather, gather_async,

    scatterto, scatterto_async,
    allgather, allgather_async,
    allreduce, allreduce_async,

    allsum,
    allmaximum,
    allminimum

### point-to-point
function sendto_async(p::Int, expr, data, mod::Module = Main)
    return @spawnat(p, Core.eval(mod, Expr(:(=), expr, data)))
end

function sendto(p::Int, expr, data, mod::Module = Main; timeout::Float64 = 5.0)
    future = sendto_async(p, expr, data, mod)
    return fetch_with_timeout(future, timeout)
end

const FutureOrNothing = Union{Future, Nothing}
function sendto_async(p::Int, mod::Module = Main; args...)
    futures = FutureOrNothing[]
    for (nm, val) in args
        future = @spawnat(p, Core.eval(mod, Expr(:(=), nm, val)))
        push!(futures, future)
    end
    return futures
end

function sendto(p::Int, mod::Module = Main; timeout::Float64 = 5.0, args...)
    futures = sendto_async(p, mod; args...)
    results = []
    for future in futures
        if future isa Future
            push!(results, fetch_with_timeout(future, timeout))
        end
    end
    return results
end

function sendto_async(p::Int, f::Function, expr, mod::Module = Main; args = ())
    if isempty(args)
        return @spawnat(p, Core.eval(mod, Expr(:call, :($f), expr)))
    else
        return @spawnat(p, Core.eval(mod, Expr(:call, :($f), expr, args...)))
    end
end

function sendto(p::Int, f::Function, expr, mod::Module = Main; timeout::Float64 = 5.0, args = ())
    future = sendto_async(p, f, expr, mod; args = args)
    e = fetch_with_timeout(future, timeout)
    if e isa Exception
        throw(e)
    end
    return e
end

function sendto_async(p::Int, f::Function, mod::Module = Main; args = ())
    if isempty(args)
        return @spawnat(p, Core.eval(mod, Expr(:call, :($f))))
    else
        return @spawnat(p, Core.eval(mod, Expr(:call, :($f), args...)))
    end
end

function sendto(p::Int, f::Function, mod::Module = Main; timeout::Float64 = 5.0, args = ())
    future = sendto_async(p, f, mod; args = args)
    e = fetch_with_timeout(future, timeout)
    if e isa Exception
        throw(e)
    end
    return e
end

macro sendto(p, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(p)), $(QuoteNode(expr)))
    end
end

function getfrom_async(p::Int, expr, mod::Module = Main)
    return @spawnat(p, Core.eval(mod, expr))
end

function getfrom(p::Int, expr, mod::Module = Main; timeout::Float64 = 5.0)
    future = getfrom_async(p, expr, mod)
    return fetch_with_timeout(future, timeout)
end

macro getfrom(p, obj, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(p)), $(QuoteNode(obj)))
    end
end

function transfer(src::Int, target::Int, from_expr, to_expr, to_mod::Module = Main, from_mod::Module = Main)
    r = RemoteChannel(src)
    @spawnat(src, put!(r, Core.eval(from_mod, from_expr)))
    @sync @spawnat(target, Core.eval(to_mod, Expr(:(=), to_expr, fetch(r))))
end

# broadcast
function bcast_async(pids::Array, expr, data, mod::Module = Main)
    if isempty(pids)
        return FutureOrNothing[]
    end
    
    futures = FutureOrNothing[]
    
    if length(pids) == 1
        future = sendto_async(pids[1], expr, data, mod)
        push!(futures, future)
    else
        mid = length(pids) ÷ 2
        left = pids[1:mid]
        right = pids[mid+1:end]
        
        left_futures = bcast_async(left, expr, data, mod)
        right_futures = bcast_async(right, expr, data, mod)
        
        append!(futures, left_futures)
        append!(futures, right_futures)
    end
    
    return futures
end

function bcast(pids::Array, expr, data, mod::Module = Main; timeout::Float64 = 5.0)
    futures = bcast_async(pids, expr, data, mod)
    for future in futures
        if future isa Future
            fetch_with_timeout(future, timeout)
        end
    end
end

function bcast_async(pids::Array, mod::Module = Main; args...)
    if isempty(pids)
        return FutureOrNothing[]
    end
    
    all_futures = FutureOrNothing[]
    
    if length(pids) == 1
        futures = sendto_async(pids[1], mod; args...)
        append!(all_futures, futures)
    else
        mid = length(pids) ÷ 2
        left = pids[1:mid]
        right = pids[mid+1:end]
        
        left_futures = bcast_async(left, mod; args...)
        right_futures = bcast_async(right, mod; args...)
        
        append!(all_futures, left_futures)
        append!(all_futures, right_futures)
    end
    
    return all_futures
end

function bcast(pids::Array, mod::Module = Main; timeout::Float64 = 5.0, args...)
    futures = bcast_async(pids, mod; args...)
    for future in futures
        if future isa Future
            fetch_with_timeout(future, timeout)
        end
    end
end

function bcast_async(pids::Array, f::Function, expr, mod::Module = Main)
    if isempty(pids)
        return FutureOrNothing[]
    end
    
    futures = FutureOrNothing[]
    
    if length(pids) == 1
        future = sendto_async(pids[1], f, expr, mod)
        push!(futures, future)
    else
        mid = length(pids) ÷ 2
        left = pids[1:mid]
        right = pids[mid+1:end]
        
        left_futures = bcast_async(left, f, expr, mod)
        right_futures = bcast_async(right, f, expr, mod)
        
        append!(futures, left_futures)
        append!(futures, right_futures)
    end
    
    return futures
end

function bcast(pids::Array, f::Function, expr, mod::Module = Main; timeout::Float64 = 5.0)
    futures = bcast_async(pids, f, expr, mod)
    for future in futures
        if future isa Future
            fetch_with_timeout(future, timeout)
        end
    end
end

function bcast_async(pids::Array, f::Function, mod::Module = Main)
    if isempty(pids)
        return FutureOrNothing[]
    end
    
    futures = FutureOrNothing[]
    
    if length(pids) == 1
        future = sendto_async(pids[1], f, mod)
        push!(futures, future)
    else
        mid = length(pids) ÷ 2
        left = pids[1:mid]
        right = pids[mid+1:end]
        
        left_futures = bcast_async(left, f, mod)
        right_futures = bcast_async(right, f, mod)
        
        append!(futures, left_futures)
        append!(futures, right_futures)
    end
    
    return futures
end

function bcast(pids::Array, f::Function, mod::Module = Main; timeout::Float64 = 5.0)
    futures = bcast_async(pids, f, mod)
    for future in futures
        if future isa Future
            fetch_with_timeout(future, timeout)
        end
    end
end

macro bcast(pids, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(pids)), $(QuoteNode(expr)))
    end
end

function scatterto_async(pids::Array, data::Array, expr, mod::Module = Main)
    if length(data) != length(pids)
        error("Unmatched data length: pids - ", length(pids), ", data - ", length(data))
    end
    
    futures = Future[]
    for i in eachindex(pids)
        future = sendto_async(pids[i], expr, data[i], mod)
        push!(futures, future)
    end
    
    return futures
end

function scatterto(pids::Array, data::Array, expr, mod::Module = Main; timeout::Float64 = 5.0)
    futures = scatterto_async(pids, data, expr, mod)
    
    for future in futures
        fetch_with_timeout(future, timeout)
    end
end

function reduce_async(f::Function, pids::Array, expr, mod::Module = Main)
    futures = Future[]
    for p in pids
        future = @spawnat(p, reduce(f, Core.eval(mod, expr)))
        push!(futures, future)
    end
    return futures
end

function Base.reduce(f::Function, pids::Array, expr, mod::Module = Main; timeout::Float64 = 5.0)
    futures = reduce_async(f, pids, expr, mod)
    
    local_results = Vector{Any}(undef, length(futures))
    @sync for (i, future) in enumerate(futures)
        @async local_results[i] = fetch_with_timeout(future, timeout)
    end
    
    return reduce(f, local_results)
end

function gather_async(pids::Array, expr, mod::Module = Main)
    futures = Future[getfrom_async(p, expr, mod) for p in pids]
    return futures
end

function gather(pids::Array, expr, mod::Module = Main; timeout::Float64 = 5.0)
    futures = gather_async(pids, expr, mod)
    results = Vector{Any}(undef, length(futures))
    
    @sync for (i, future) in enumerate(futures)
        @async results[i] = fetch_with_timeout(future, timeout)
    end
    
    return results
end

macro gather(pids, expr, mod::Symbol = :Main)
    quote
        [getfrom(p, $(QuoteNode(expr)), $(esc(mod))) for p in $(esc(pids))]
    end
end

function gather_async(f::Function, pids::Array, mod::Module = Main; args...)
    futures = Future[]
    for p in pids
        future = @spawnat(1, sendto(p, f, mod; args...))
        push!(futures, future)
    end
    return futures
end

function gather(f::Function, pids::Array, mod::Module = Main; timeout::Float64 = 5.0, args...)
    futures = gather_async(f, pids, mod; args...)
    results = Vector{Any}(undef, length(futures))
    
    @sync for (i, future) in enumerate(futures)
        @async results[i] = fetch_with_timeout(future, timeout)
    end
    
    return results
end

function gather_async(f::Function, pids::Array, expr, mod::Module = Main; args...)
    futures = Future[]
    for p in pids
        future = @spawnat(1, sendto(p, f, expr, mod; args...))
        push!(futures, future)
    end
    return futures
end

function gather(f::Function, pids::Array, expr, mod::Module = Main; timeout::Float64 = 5.0, args...)
    futures = gather_async(f, pids, expr, mod; args...)
    results = Vector{Any}(undef, length(futures))
    
    @sync for (i, future) in enumerate(futures)
        @async results[i] = fetch_with_timeout(future, timeout)
    end
    
    return results
end

function allgather_async(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main)
    gather_futures = gather_async(pids, src_expr, mod)
    gather_result = fetch.(gather_futures)
    bcast_futures = bcast_async(pids, target_expr, gather_result, mod)
    
    return bcast_futures
end

function allgather(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main; timeout::Float64 = 5.0)
    result = gather(pids, src_expr, mod; timeout = timeout)
    bcast(pids, target_expr, result, mod; timeout = timeout)
end

function allreduce_async(f::Function, pids::Array, src_expr, target_expr = src_expr, mod::Module = Main)
    reduce_futures = reduce_async(f, pids, src_expr, mod)
    reduce_result = reduce(f, fetch.(reduce_futures))
    bcast_futures = bcast_async(pids, target_expr, reduce_result, mod)
    
    return bcast_futures
end

function allreduce(f::Function, pids::Array, src_expr, target_expr = src_expr, mod::Module = Main; timeout::Float64 = 5.0)
    result = reduce(f, pids, src_expr, mod; timeout = timeout)
    bcast(pids, target_expr, result, mod; timeout = timeout)
end

### Commonly used functions
function _allsum(pids::Array, expr::Union{Symbol, Expr}, mod::Module = Main)
    return Base.sum(gather(pids, expr, mod))
end

allsum(pids::Array, src_expr::Union{Symbol, Expr}, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, _allsum(pids, src_expr, mod), mod)

function _allmaximum(pids::Array, expr, mod::Module = Main)
    return Base.maximum(gather(pids, expr, mod))
end

allmaximum(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, _allmaximum(pids, src_expr, mod), mod)

function _allminimum(pids::Array, expr, mod::Module = Main)
    return Base.minimum(gather(pids, expr, mod))
end

allminimum(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, _allminimum(pids, src_expr, mod), mod)

end
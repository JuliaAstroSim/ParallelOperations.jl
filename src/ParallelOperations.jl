module ParallelOperations

using Distributed

import Base: reduce, sum, maximum, minimum

export
    sendto, @sendto,
    getfrom, @getfrom,
    transfer,

    bcast, @bcast,
    reduce,
    gather, @gather,

    scatter,
    allgather,
    allreduce,

    sum, allsum,
    maximum, allmaximum,
    minimum, allminimum

# point-to-point

function sendto(p::Int, expr, data, mod::Module = Main)
    @sync @spawnat(p, Core.eval(mod, Expr(:(=), expr, data)))
end

function sendto(p::Int, mod::Module = Main; args...)
    @sync for (nm, val) in args
        @spawnat(p, Core.eval(mod, Expr(:(=), nm, val)))
    end
end

function sendto(p::Int, f::Function, expr, mod::Module = Main)
    fetch(@spawnat(p, Core.eval(mod, Expr(:call, :($f), expr))))
end

function sendto(p::Int, f::Function, mod::Module = Main)
    expr = Meta.parse("function " * string(f) * " end")
    Distributed.remotecall_eval(mod, p, expr)

    for m in methods(f)
        Distributed.remotecall_eval(mod, p, m)
    end
end

macro sendto(p, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(p)), $(QuoteNode(expr)))
    end
end

function getfrom(p::Int, expr, mod::Module = Main)
    return fetch(@spawnat(p, Core.eval(mod, expr)))
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

function bcast(pids::Array, expr, data, mod::Module = Main)
    for p in pids
        sendto(p, expr, data, mod)
    end
end

function bcast(pids::Array, mod::Module = Main; args...)
    for p in pids
        sendto(p, mod; args...)
    end
end

function bcast(pids::Array, f::Function, expr, mod::Module = Main)
    for p in pids
        sendto(p, f, expr, mod)
    end
end

function bcast(pids::Array, f::Function, mod::Module = Main)
    expr = Meta.parse("function " * string(f) * " end")
    Distributed.remotecall_eval(mod, pids, expr)

    for m in methods(f)
        Distributed.remotecall_eval(mod, pids, m)
    end
end

macro bcast(pids, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(pids)), $(QuoteNode(expr)))
    end
end

# scatter

function scatter(pids::Array, data::Array, expr, mod::Module = Main)
    if length(data) == length(pids)
        for i in eachindex(pids)
            @inbounds sendto(pids[i], expr, data[i], mod)
        end
    else
        error("Unmatched data length: pids - ", length(pids), ", data - ", length(data))
    end
end

# Reduce

function Base.reduce(f::Function, pids::Array, expr, mod::Module = Main)
    results = map(pids) do p
        remotecall_fetch(p) do
            return reduce(f, Core.eval(mod, expr))
        end
    end
    return reduce(f, results)
end

# Gather

function gather(pids::Array, expr, mod::Module = Main)
    return [fetch(@spawnat(p, Core.eval(mod, expr))) for p in pids]
end

macro gather(pids, expr, mod::Symbol = :Main)
    quote
        [getfrom(p, $(QuoteNode(expr)), $(esc(mod))) for p in $(esc(pids))]
    end
end

gather(f::Function, pids::Array, expr, mod::Module = Main) = gather(pids, :($f($expr)), mod)

# allgather

function allgather(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main)
    result = gather(pids, src_expr, mod)
    bcast(pids, target_expr, result, mod)
end

# allreduce

function allreduce(f::Function, pids::Array, src_expr, target_expr = src_expr, mod::Module = Main)
    result = reduce(f, pids, src_expr, mod)
    bcast(pids, target_expr, result, mod)
end

# Commonly used functions
sum(pids::Array, expr, mod::Module = Main) = sum(gather(pids, expr, mod))
allsum(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, sum(pids, src_expr, mod), mod)

maximum(pids::Array, expr, mod::Module = Main) = maximum(gather(pids, expr, mod))
allmaximum(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, maximum(pids, src_expr, mod), mod)

minimum(pids::Array, expr, mod::Module = Main) = minimum(gather(pids, expr, mod))
allminimum(pids::Array, src_expr, target_expr = src_expr, mod::Module = Main) = bcast(pids, target_expr, minimum(pids, src_expr, mod), mod)

end
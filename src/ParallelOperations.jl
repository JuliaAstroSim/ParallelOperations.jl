module ParallelOperations

using Distributed

import Base: reduce

export
    sendto, @sendto,
    getfrom, @getfrom,
    transfer,

    bcast, @bcast,
    reduce,
    gather, @gather,

    scatter,
    allgather,
    allreduce

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
    asyncmap(pids) do p
        sendto(p, expr, data, mod)
    end
end

function bcast(pids::Array, mod::Module = Main; args...)
    asyncmap(pids) do p
        sendto(p, mod; args...)
    end
end

function bcast(pids::Array, f::Function, expr, mod::Module = Main)
    asyncmap(pids) do p
        sendto(p, f, expr, mod)
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
    results = asyncmap(pids) do p
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

function allgather(pids::Array, src_expr, mod::Module = Main)
    result = gather(pids, src_expr, mod)
    bcast(pids, src_expr, result, mod)
end

function allgather(pids::Array, src_expr, target_expr, mod::Module = Main)
    result = gather(pids, src_expr, mod)
    bcast(pids, target_expr, result, mod)
end

# allreduce

function allreduce(f::Function, pids::Array, src_expr, mod::Module = Main)
    result = reduce(f, pids, src_expr, mod)
    bcast(pids, src_expr, result, mod)
end

function allreduce(f::Function, pids::Array, src_expr, target_expr, mod::Module = Main)
    result = reduce(f, pids, src_expr, mod)
    bcast(pids, target_expr, result, mod)
end

end
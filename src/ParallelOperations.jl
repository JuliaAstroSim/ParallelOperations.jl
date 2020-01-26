module ParallelOperations

using Distributed

import Base: reduce

export
    sendto,
    getfrom,
    transfer,

    bcast,
    reduce,
    gather

# point-to-point

function sendto(p::Int, expr, data, mod = Main)
    @sync @spawnat(p, Core.eval(mod, Expr(:(=), expr, data)))
end

function sendto(p::Int, mod = Main; args...)
    @sync for (nm, val) in args
        @spawnat(p, Core.eval(mod, Expr(:(=), nm, val)))
    end
end

function sendto(p::Int, f::Function, expr, mod = Main)
    fetch(@spawnat(p, Core.eval(mod, Expr(:call, :(f), expr))))
end

function getfrom(p::Int, expr, mod = Main)
    return fetch(@spawnat(p, Core.eval(mod, expr)))
end

function transfer(src::Int, target::Int, from_expr, to_expr, to_mod = Main, from_mod = Main)
    r = RemoteChannel(src)
    @spawnat(src, put!(r, Core.eval(from_mod, from_expr)))
    @sync @spawnat(target, Core.eval(to_mod, Expr(:(=), to_expr, fetch(r))))
end

# broadcast

function bcast(pids::Array, expr, data, mod = Main)
    asyncmap(pids) do p
        sendto(p, expr, data, mod)
    end
end

function bcast(pids::Array, mod = Main; args...)
    asyncmap(pids) do p
        sendto(p, mod; args...)
    end
end

function bcast(pids::Array, f::Function, expr, mod = Main)
    asyncmap(pids) do p
        sendto(p, f, expr, mod)
    end
end

# Reduce

function Base.reduce(f::Function, pids::Array, expr, mod = Main)
    results = asyncmap(pids) do p
        remotecall_fetch(p) do
            return reduce(f, Core.eval(mod, expr))
        end
    end
    return reduce(f, results)
end

# Gather

function gather(pids::Array, expr, mod=Main)
    results = asyncmap(pids) do p
        fetch(@spawnat(p, Core.eval(mod, expr)))
    end
    return results
end

gather(pids::Array, f::Function, expr, mod=Main) = gather(pids, :($f($expr)), mod)

end
module ParallelOperations

using Distributed

import Base: reduce

export
    reduce,
    gather

# Reduce

function Base.reduce(f::Function, pids::Array, symbol::Symbol, mod = Main)
    results = asyncmap(pids) do p
        remotecall_fetch(p) do
            return reduce(f, Core.eval(mod, symbol))
        end
    end
    return reduce(f, results)
end

function Base.reduce(f::Function, pids::Array, expr::Expr, mod = Main)
    results = asyncmap(pids) do p
        remotecall_fetch(p) do
            return reduce(f, Core.eval(mod, expr))
        end
    end
    return reduce(f, results)
end

# Gather

function gather(pids::Array, expr::Expr, mod=Main)
    results = asyncmap(pids) do p
        fetch(@spawnat(p, Core.eval(mod, expr)))
    end
    return results
end

function gather(pids::Array, symbol::Symbol, mod=Main)
    results = asyncmap(pids) do p
        fetch(@spawnat(p, getfield(mod, symbol)))
    end
    return results
end

gather(f::Function, pids::Array{Integer}, expr::Expr, mod=Main) = gather(pids, :($f($expr)), mod)
gather(f::Function, pids::Array{Integer}, symbol::Symbol, mod=Main) = gather(pids, :($f($symbol)), mod)

# Communication

const sendlist = Dict()
const recvlist = Dict()

end
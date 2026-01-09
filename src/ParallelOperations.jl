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

    scatterto,
    allgather,
    allreduce,

    allsum,
    allmaximum,
    allminimum

# point-to-point

function sendto(p::Int, expr, data, mod::Module = Main)
    remotecall_fetch(p) do
        Core.eval(mod, Expr(:(=), expr, data))
    end
end

function sendto(p::Int, mod::Module = Main; args...)
    for (nm, val) in args
        remotecall_fetch(p) do
            Core.eval(mod, Expr(:(=), nm, val))
        end
    end
end

function sendto(p::Int, f::Function, expr, mod::Module = Main; args = ())
    e = fetch(@spawnat(p, Core.eval(mod, Expr(:call, :($f), expr, :($args...)))))
    if e isa Exception
        throw(e)
    end
    return e
end

function sendto(p::Int, f::Function, mod::Module = Main; args = ())
    #expr = Meta.parse("function " * string(f) * " end")
    #Distributed.remotecall_eval(mod, p, expr)

    #for m in methods(f)
        e = fetch(@spawnat(p, Core.eval(mod, Expr(:call, :($f), :($args...)))))
        if e isa Exception
            throw(e)
        end
        #Distributed.remotecall_eval(mod, p, m)
    #end
    return e
end

macro sendto(p, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(p)), $(QuoteNode(expr)))
    end
end

function getfrom(p::Int, expr, mod::Module = Main)
    return remotecall_fetch(p) do
        Core.eval(mod, expr)
    end
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
    @sync for p in pids
        sendto(p, expr, data, mod)
    end
end

function bcast(pids::Array, mod::Module = Main; args...)
    @sync for p in pids
        sendto(p, mod; args...)
    end
end

function bcast(pids::Array, f::Function, expr, mod::Module = Main; args...)
    @sync for p in pids
        sendto(p, f, expr, mod; args...)
    end
end

function bcast(pids::Array, f::Function, mod::Module = Main; args...)
    @sync for p in pids
        sendto(p, f, mod; args...)
    end
end

macro bcast(pids, expr, mod::Symbol = :Main)
    quote
        Distributed.remotecall_eval($(esc(mod)), $(esc(pids)), $(QuoteNode(expr)))
    end
end

# scatter

function scatterto(pids::Array, data::Array, expr, mod::Module = Main)
    if length(data) == length(pids)
        @sync for i in eachindex(pids)
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
    # return [fetch(@spawnat(p, Core.eval(mod, expr))) for p in pids]
    results = Vector{Any}(undef, length(pids))
    @sync for (i, p) in enumerate(pids)
        @async results[i] = fetch(@spawnat(p, Core.eval(mod, expr)))
    end
    return results
end

macro gather(pids, expr, mod::Symbol = :Main)
    quote
        [getfrom(p, $(QuoteNode(expr)), $(esc(mod))) for p in $(esc(pids))]
    end
end

gather(f::Function, pids::Array, mod::Module = Main; args...) = [sendto(p, f, mod; args...) for p in pids]
gather(f::Function, pids::Array, expr, mod::Module = Main; args...) = [sendto(p, f, expr, mod; args...) for p in pids]

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
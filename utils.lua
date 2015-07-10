module('utils', package.seeall)

function shell(cmd)
    local output = io.popen(cmd):read()
    return output
end

function pwd()
    return shell('pwd')
end

function long_bit()
    return shell('getconf LONG_BIT')
end

function path_join(path, sub_path)
    return path..'/'..sub_path
end

function add_cpath(path)
    package.cpath = path..'/?.so'
end

function add_lpath(path)
    package.path = path..'/?.lua'
end

module('pool', package.seeall)

local sql = require("luasql.mysql").mysql()
local ini_parser = require('LIP')
local proxy_log = require("logging").proxy_log

Pool = {}
Pool.__index = Pool
function Pool:new()
    self = {}
    setmetatable(self, Pool)
    self.backup = {}
    self.pool = {}
    return self
end

function Pool:setup(config_file)
    proxy_log:debug("config file:" .. config_file)
    local conf = ini_parser.load(config_file)
    local sect = 'mysql-proxy'
    local user = conf[sect]['admin-username']
    local passwd = conf[sect]['admin-password']
    local master = conf[sect]['proxy-backend-addresses']
    local slaves = conf[sect]['proxy-read-only-backend-addresses']
    local i = 1

    proxy_log:debug("user name:" .. user)
    proxy_log:debug("password:" .. passwd)
    proxy_log:debug("rw backends:" .. master)
    proxy_log:debug("ro backends:" .. slaves)

    for m in string.gmatch(master, "[^,]+") do
        local addr, port = string.match(m, "(.+):(%w+)")
	proxy_log:debug("set address:" .. addr .. " port:" .. port )
        local con = sql:connect("", user, passwd, addr, tonumber(port))
        self.pool[m] = con
        self.backup[m] = {user, passwd, addr, port}
    end

    for s in string.gmatch(slaves, "[^,]+") do
        local addr, port = string.match(s, "(.+):(%w+)")
	proxy_log:debug("set address:" .. addr .. " port:" .. port )
        local con = sql:connect("", user, passwd, addr, tonumber(port))
        self.pool[s] = con
        self.backup[s] = {user, passwd, addr, port}
    end
    
    proxy_log:debug(table.getn(self.pool))
end

function Pool:_get(cursor, item_name)
    local tb = {}
    local row = {}
    while row do
        row = cursor:fetch(row, 'a')
        if row then
            for k, v in pairs(row) do
	        proxy_log:debug("get key:" .. k)
                if item_name == k then
	            proxy_log:debug("get value:" .. v)
                    return v
                end
                tb[k] = v
            end
        else
            if table.getn(tb) == 0 then
	        proxy_log:debug("get value: \'\'")
                return ""
            else
                return tb
            end
        end
    end
end

function Pool:_reconnect(name)
    local server = self.backup[name]
    con = sql:connect("", server[1], server[2], server[3], server[4])  
    return con
end

function Pool:query(name, cmd, item_name)
    proxy_log:debug(table.getn(self.pool))
    local con = self.pool[name]
    if con then
	proxy_log:debug("execute command:" .. cmd)
        local cursor = con:execute(cmd)
        if cursor == 2 then
            con = self._reconnect(name)
            if con then
                cursor = con:execute(cmd)
                if cursor == 2 then
                    return nil
                else
                    return self:_get(cursor, item_name)
                end
            else
                return nil
            end
        else
            return self:_get(cursor, item_name)
        end
    end
end

lua_pool = Pool:new()

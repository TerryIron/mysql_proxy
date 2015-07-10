module('mysql-dev', package.seeall)

local utils = require('utils')
local lua_pool = require('pool').lua_pool
local proxy_log = require('logging').proxy_log

-- GET_MASTER_POS = 10
-- GET_MASTER_DATABASE = 11

function get_master_pos(id)
    local cmd = 'show master status'
    local item = 'Position'
    proxy_log:debug("send id:" .. id .. " cmd:" .. cmd) 
    local d = lua_pool:query(id, cmd, item)

    return d
end

function get_slave_pos(id)
    local cmd = 'show slave status'
    local item = 'Exec_Master_Log_Pos'
    proxy_log:debug("send id:" .. id .. " cmd:" .. cmd) 
    local d = lua_pool:query(id, cmd, item)

    return d
end

function get_cur_database(id)
    local cmd = 'select database()'
    local item = 'database()'
    proxy_log:debug("send id:" .. id .. " cmd:" .. cmd) 
    local d = lua_pool:query(id, cmd, item)

    return d
end

Mysql = {}
Mysql.__index = Mysql
function Mysql:new()
    self = {}
    setmetatable(self, Mysql)
    self.trusty_slave = {}
    self.untrusty_slave = {}
    self.setup_is_done = false
    self.master_name = ''
    self.master_pos_buf = 107
    self.master_database = ''
    self.master_table = ''
    self.table = {}
--    self.query = {}
--    self.query[GET_MASTER_POS] = 0
--    self.query[GET_MASTER_DATABASE] = 0
    return self
end

function Mysql:_update_pos(name, db_name, table_name, pos)
    local key = name .. '.' .. db_name .. '.' .. table_name
    self.table[key] = pos
end

function Mysql:_search_pos(name, db_name, table_name)
    local key = name .. '.' .. db_name .. '.' .. table_name
    if self.table[key] then
        return self.table[key]
    end
end

function Mysql:update_slave(name)
    local pos = get_slave_pos(name)

    if pos then
        self:_update_pos(name, self.master_database, self.master_table, pos)
    end 

    return pos
end

function Mysql:update_master(name)   
    local pos = get_master_pos(name)
    local database = get_cur_database(name)

    if pos and database then
        self.master_pos_buf = pos
        self.master_database = database
    end

    return pos, database
end

function Mysql:set_master_name(name)
    self.master_name = name
end

function Mysql:get_master_name(name)
    return self.master_name
end

function Mysql:get_master_pos()
    return self.master_pos_buf
end

function Mysql:check_slave(name)
    local m_pos = self.master_pos_buf
    local s_pos = self:_search_pos(name, self.master_database, self.master_table)
    if s_pos and m_pos and tonumber(s_pos) >= tonumber(m_pos) then
        return true
    end
    return false
end

function Mysql:init()
    self.trusty_slave = {}
    self.untrusty_slave = {}
    self.setup_is_done = false
end

function Mysql:set_trusty_slave(name, slave)
    self.trusty_slave[name] = slave 
end

function Mysql:get_trusty_slave(name)
    return self.trusty_slave[name]
end

function Mysql:set_untrusty_slave(name, slave)
    self.untrusty_slave[name] = slave 
end

function Mysql:get_untrusty_slave(name)
    return self.untrusty_slave[name]
end

function Mysql:is_setup()
    return self.setup_is_done
end

mysql_dev = Mysql:new()

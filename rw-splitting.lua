--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ --]]

---
-- a flexible statement based load balancer with connection pooling
--
-- * build a connection pool of min_idle_connections for each backend and maintain
--   its size
-- * 
-- 
--
local ini_parser = require('LIP')
local proxy_log = require('logging').proxy_log
local mysql_dev = require('mysql-dev').mysql_dev
local mysql = require('mysql-dev')
local lua_pool = require("pool").lua_pool
local balance = require('proxy.balance')
local commands = require("proxy.commands")
local tokenizer = require("proxy.tokenizer")
local auto_config = require("proxy.auto-config")

local CONF = ini_parser.load('./mysql-proxy-lua.conf')
local DEFAULT = "DEFAULT"
local USER_CONF = CONF[DEFAULT]["config"]


--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
    proxy.global.config.rwsplit = {}
    proxy.global.config.rwsplit["min_idle_connections"] = CONF[DEFAULT]["min_connections"]
    proxy.global.config.rwsplit["max_idle_connections"] = CONF[DEFAULT]["max_connections"]
    proxy.global.config.rwsplit["is_debug"] = true
end

proxy_log:set_level(CONF[DEFAULT]["log_level"])
proxy_log:set_path(CONF[DEFAULT]["log_path"])

if USER_CONF ~= '' and USER_CONF ~= nil then
	lua_pool:setup(USER_CONF)
else
	proxy_log:crit("Lost config file to initial mysql-proxy pool.")
end

mysql_dev:init()

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction       = false

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
    local is_debug = proxy.global.config.rwsplit.is_debug
    -- make sure that we connect to each backend at least ones to
    -- keep the connections to the servers alive
    --
    -- on read_query we can switch the backends again to another backend

    if is_debug then
        proxy_log:debug()
        proxy_log:debug("[connect_server] " .. proxy.connection.client.src.name)
    end

    local rw_ndx = 0

    -- init all backends
    for i = 1, #proxy.global.backends do
        local s        = proxy.global.backends[i]
        local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
        local cur_idle = pool.users[""].cur_idle_connections

        pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
        pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections

        if is_debug then
            proxy_log:debug("  [".. i .."].connected_clients = " .. s.connected_clients)
            proxy_log:debug("  [".. i .."].pool.cur_idle     = " .. cur_idle)
            proxy_log:debug("  [".. i .."].pool.max_idle     = " .. pool.max_idle_connections)
            proxy_log:debug("  [".. i .."].pool.min_idle     = " .. pool.min_idle_connections)
            proxy_log:debug("  [".. i .."].type = " .. s.type)
            proxy_log:debug("  [".. i .."].state = " .. s.state)
        end

        -- prefer connections to the master
        if s.type == proxy.BACKEND_TYPE_RW and
           s.state ~= proxy.BACKEND_STATE_DOWN and
           cur_idle < pool.min_idle_connections then
            proxy.connection.backend_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RO and
               s.state ~= proxy.BACKEND_STATE_DOWN and
               cur_idle < pool.min_idle_connections then
            proxy.connection.backend_ndx = i
            break
        elseif s.type == proxy.BACKEND_TYPE_RW and
               s.state ~= proxy.BACKEND_STATE_DOWN and
               rw_ndx == 0 then
            rw_ndx = i
        end
    end

    if proxy.connection.backend_ndx == 0 then
        if is_debug then
            proxy_log:debug("  [" .. rw_ndx .. "] taking master as default")
        end
        proxy.connection.backend_ndx = rw_ndx
    end

    -- pick a random backend
    --
    -- we someone have to skip DOWN backends

    -- ok, did we got a backend ?

    if proxy.connection.server then
        if is_debug then
            proxy_log:debug("  using pooled connection from: " .. proxy.connection.backend_ndx)
        end

        -- stay with it
        return proxy.PROXY_IGNORE_RESULT
    end

    if is_debug then
        proxy_log:debug("  [" .. proxy.connection.backend_ndx .. "] idle-conns below min-idle")
    end

    -- open a new connection
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
    if is_debug then
        proxy_log:debug("[read_auth_result] " .. proxy.connection.client.src.name)
    end
    if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
        -- auth was fine, disconnect from the server
        proxy.connection.backend_ndx = 0
    elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
        -- we received either a
        --
        -- * MYSQLD_PACKET_ERR and the auth failed or
        -- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
        proxy_log:info("(read_auth_result) ... not ok yet");
    elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
        -- auth failed
    end
end


--- 
-- read/write splitting
function read_query( packet )
    local is_debug = proxy.global.config.rwsplit.is_debug
    local cmd      = commands.parse(packet)
    local c        = proxy.connection.client

    local r = auto_config.handle(cmd)
    if r then return r end

    local tokens
    local norm_query

    -- looks like we have to forward this statement to a backend
    if is_debug then
        proxy_log:debug("[read_query] " .. proxy.connection.client.src.name)
        proxy_log:debug("  current backend   = " .. proxy.connection.backend_ndx)
        proxy_log:debug("  client default db = " .. c.default_db)
        proxy_log:debug("  client username   = " .. c.username)
        if cmd.type == proxy.COM_QUERY then
            proxy_log:debug("  query             = "        .. cmd.query)
        end
    end

    if cmd.type == proxy.COM_QUIT then
        -- don't send COM_QUIT to the backend. We manage the connection
        -- in all aspects.
        proxy.response = {
            type = proxy.MYSQLD_PACKET_OK,
        }

        if is_debug then
            proxy_log:debug("  (QUIT) current backend   = " .. proxy.connection.backend_ndx)
        end

        return proxy.PROXY_SEND_RESULT
    end

    -- read/write splitting
    --
    -- send all non-transactional SELECTs to a slave
    if not is_in_transaction  then
       if cmd.type == proxy.COM_QUERY then
            tokens = tokens or assert(tokenizer.tokenize(cmd.query))

            local stmt = tokenizer.first_stmt_token(tokens)

            if stmt.token_name == "TK_SQL_SELECT" then
                is_in_select_calc_found_rows = false
                local is_insert_id = false

                for i = 1, #tokens do
                    local token = tokens[i]
                    -- SQL_CALC_FOUND_ROWS + FOUND_ROWS() have to be executed
                    -- on the same connection
                    -- proxy_log:info("token: " .. token.token_name)
                    -- proxy_log:info("  val: " .. token.text)

                    if not is_in_select_calc_found_rows and token.token_name == "TK_SQL_SQL_CALC_FOUND_ROWS" then
                        is_in_select_calc_found_rows = true
                    elseif not is_insert_id and token.token_name == "TK_LITERAL" then
                        local utext = token.text:upper()

                        if utext == "LAST_INSERT_ID" or
                            utext == "@@INSERT_ID" then
                            is_insert_id = true
                        end
                    end

                    -- we found the two special token, we can't find more
                    if is_insert_id and is_in_select_calc_found_rows then
                        break
                    end
                end

                -- if we ask for the last-insert-id we have to ask it on the original
                -- connection
                if not is_insert_id then
                    local backend_ndx = balance.idle_ro()

                    if backend_ndx > 0 then
                        proxy.connection.backend_ndx = backend_ndx
                    end
                else
                    proxy_log:info("   found a SELECT LAST_INSERT_ID(), staying on the same backend")
                end
            end
        end
    end

    -- no backend selected yet, pick a master
    if proxy.connection.backend_ndx == 0 then
        -- we don't have a backend right now
        --
        -- let's pick a master as a good default
        --
        proxy.connection.backend_ndx = balance.idle_failsafe_rw()
    end
    proxy.queries:append(1, packet, { resultset_is_needed = true })

    -- by now we should have a backend
    --
    -- in case the master is down, we have to close the client connections
    -- otherwise we can go on
    if proxy.connection.backend_ndx == 0 then
        return proxy.PROXY_SEND_QUERY
    end

    local s = proxy.connection.server

    -- if client and server db don't match, adjust the server-side
    --
    -- skip it if we send a INIT_DB anyway
    if cmd.type ~= proxy.COM_INIT_DB and
        c.default_db and c.default_db ~= "" and c.default_db ~= s.default_db then
        proxy_log:info("    server default db: " .. s.default_db)
        proxy_log:info("    client default db: " .. c.default_db)
        proxy_log:info("    syncronizing")
        proxy.queries:prepend(2, string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })
    end

    -- send to master
    if is_debug then
        if proxy.connection.backend_ndx > 0 then
            local b = proxy.global.backends[proxy.connection.backend_ndx]
            proxy_log:debug("  sending to backend : " .. b.dst.name);
            proxy_log:debug("    is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO));
            -- proxy_log:debug("    server default db: " .. c.default_db)
            -- proxy_log:debug("    server username  : " .. c.username)
        end
        proxy_log:debug("    in_trans        : " .. tostring(is_in_transaction))
        proxy_log:debug("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
        proxy_log:debug("    COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY))
    end

    return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj )
    local is_debug = proxy.global.config.rwsplit.is_debug
    local res      = assert(inj.resultset)
    local flags    = res.flags

    -- if inj.id == mysql.GET_MASTER_DATABASE then
    --     for row in inj.resultset.rows do
    --         mysql_dev.master_database = row[1]
    --         break
    --     end
    --     mysql_dev.query[mysql.GET_MASTER_DATABASE] = mysql_dev.query[mysql.GET_MASTER_DATABASE] + 1
    -- end

    -- if inj.id == mysql.GET_MASTER_POS then
    --     for row in inj.resultset.rows do
    --         mysql_dev.master_pos_buf = (row[2])
    --         break
    --     end
    --     mysql_dev.query[mysql.GET_MASTER_POS] = mysql_dev.query[mysql.GET_MASTER_POS] + 1
    -- end

    if inj.id ~= 1 then
        -- ignore the result of the USE <default_db>
        -- the DB might not exist on the backend, what do do ?
        --
        if inj.id == 2 then
            -- the injected INIT_DB failed as the slave doesn't have this DB
            -- or doesn't have permissions to read from it
            if res.query_status == proxy.MYSQLD_PACKET_ERR then
                proxy.queries:reset()

                proxy.response = {
                    type = proxy.MYSQLD_PACKET_ERR,
                    errmsg = "can't change DB ".. proxy.connection.client.default_db ..
                        " to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
                }

                return proxy.PROXY_SEND_RESULT
            end
        end
        return proxy.PROXY_IGNORE_RESULT
    end

    is_in_transaction = flags.in_trans
    local have_last_insert_id = (res.insert_id and (res.insert_id > 0))

    if not is_in_transaction and
       not is_in_select_calc_found_rows and
       not have_last_insert_id then
        -- release the backend
        -- proxy_log:debug("Master database count: " .. mysql_dev.query[mysql.GET_MASTER_DATABASE])
        -- proxy_log:debug("Master position count: " .. mysql_dev.query[mysql.GET_MASTER_POS])
        -- if mysql_dev.query[mysql.GET_MASTER_DATABASE] == mysql_dev.query[mysql.GET_MASTER_POS] then
        --     if mysql_dev.master_database then
        --         proxy_log:debug("Master database: " .. mysql_dev.master_database)
        --     end
        --     if mysql_dev.master_pos_buf then
        --         proxy_log:debug("Master new position: " .. mysql_dev.master_pos_buf)
        --     end
        --     proxy.connection.backend_ndx = 0
        -- end
        proxy.connection.backend_ndx = 0
    elseif is_debug then
        proxy_log:debug("(read_query_result) staying on the same backend")
        proxy_log:debug("    in_trans        : " .. tostring(is_in_transaction))
        proxy_log:debug("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
        proxy_log:debug("    have_insert_id  : " .. tostring(have_last_insert_id))
    end
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
    local is_debug = proxy.global.config.rwsplit.is_debug
    if is_debug then
        proxy_log:debug("[disconnect_client] " .. proxy.connection.client.src.name)
    end

    -- make sure we are disconnection from the connection
    -- to move the connection into the pool
    proxy.connection.backend_ndx = 0
end


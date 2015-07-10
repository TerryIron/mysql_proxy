--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

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


module("proxy.balance", package.seeall)
local mysql_dev = require('mysql-dev').mysql_dev
local proxy_log = require('logging').proxy_log

function idle_failsafe_rw()
    local backend_ndx = 0

    for i = 1, #proxy.global.backends do
        local s = proxy.global.backends[i]
        local conns = s.pool.users[proxy.connection.client.username]

        if conns.cur_idle_connections > 0 and
           s.state ~= proxy.BACKEND_STATE_DOWN and
           s.type == proxy.BACKEND_TYPE_RW then
            backend_ndx = i
            mysql_dev:set_master_name(s.dst.name)
        end
    end

    return backend_ndx
end

function idle_ro()
    local max_conns = -1
    local max_conns_ndx = 0

    if not mysql_dev:is_setup() then
        for i = 1, #proxy.global.backends do
            local s = proxy.global.backends[i]
            if s.type == proxy.BACKEND_TYPE_RO then
                mysql_dev:set_untrusty_slave(s.dst.name, {s, i})
            end
        end
    end

    local master_name = mysql_dev:get_master_name()
    proxy_log:debug("master name:" .. master_name)
    mysql_dev:update_master(master_name)
    for name, s_group in pairs(mysql_dev.trusty_slave) do
        local s = s_group[1]
        local i = s_group[2]
        local conns = s.pool.users[proxy.connection.client.username]
        if s.type == proxy.BACKEND_TYPE_RO and
           s.state ~= proxy.BACKEND_STATE_DOWN and
           conns.cur_idle_connections > 0 then
            local pos = mysql_dev:update_slave(name)
            proxy_log:debug("master position:" .. mysql_dev.master_pos_buf)
            proxy_log:debug("trusty slave position:" .. pos)
            if pos and mysql_dev:check_slave(slave_name) then
                if max_conns == -1 or
                   s.connected_clients < max_conns then
                    return i
                end
            end
        end
    end

    for name, s_group in pairs(mysql_dev.untrusty_slave) do
        local s = s_group[1]
        local i = s_group[2]
        local conns = s.pool.users[proxy.connection.client.username]

        -- pick a slave which has some idling connections
        if s.type == proxy.BACKEND_TYPE_RO and
           s.state ~= proxy.BACKEND_STATE_DOWN and
           conns.cur_idle_connections >= 0 then
            local pos = mysql_dev:update_slave(name)
            proxy_log:debug("master position:" .. mysql_dev:get_master_pos())
            proxy_log:debug("untrusty slave position:" .. pos)
            if pos and mysql_dev:check_slave(name) then
                if max_conns == -1 or
                   s.connected_clients < max_conns then
                    return i
                    -- max_conns = s.connected_clients
                    -- max_conns_ndx = i
                end
            end
        end
    end

    return max_conns_ndx
end

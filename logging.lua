module('logging', package.seeall)

local default_log_level = {
    NOTSET = 60,
    CRITICAL = 50,
    ERROR = 40,
    WARNING = 30,
    INFO = 20,
    DEBUG = 10
}

logging = {}
logging.__index = logging
function logging:new()
    local self = {}
    setmetatable(self, logging)
    self.log_level = default_log_level.INFO
    self.log_file = nil
    self.log_format = '[%s] [%s] %s'
    return self
end


function logging:set_level(level)
    if level == 'debug' then
        self.log_level = default_log_level.DEBUG
    elseif level == 'info' then
        self.log_level = default_log_level.INFO
    elseif level == 'warn' then
        self.log_level = default_log_level.WARNING
    elseif level == 'error' then
        self.log_level = default_log_level.ERROR
    elseif level == 'crit' then
        self.log_level = default_log_level.CRITICAL
    elseif level == 'nil' then
        self.log_level = default_log_level.NOTSET
    end
end

function logging:set_path(path)
    self.log_file = io.open(path, 'a+')
end

function logging:info(msg)
    if self.log_file and msg then
        if self.log_level <= default_log_level.INFO then
            local str = string.format(self.log_format, os.date(), 'INFO', msg)
            self.log_file:write(str..'\n')
        end
    end
end

function logging:debug(msg)
    if self.log_file and msg then
        if self.log_level <= default_log_level.DEBUG then
            local str = string.format(self.log_format, os.date(), 'DEBUG', msg)
            self.log_file:write(str..'\n')
        end
    end
end

function logging:error(msg)
    if self.log_file and msg then
        if self.log_level <= default_log_level.ERROR then
            local str = string.format(self.log_format, os.date(), 'ERROR', msg)
            self.log_file:write(str..'\n')
        end
    end
end

function logging:warn(msg)
    if self.log_file and msg then
        if self.log_level <= default_log_level.WARNING then
            local str = string.format(self.log_format, os.date(), 'WARN', msg)
            self.log_file:write(str..'\n')
        end
    end
end

function logging:crit(msg)
    if self.log_file and msg then
        if self.log_level <= default_log_level.CRITICAL then
            local str = string.format(self.log_format, os.date(), 'CRIT', msg)
            self.log_file:write(str..'\n')
        end
    end
end

proxy_log = logging:new()

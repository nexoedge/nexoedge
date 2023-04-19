#!/bin/bash

#######
## Script for manually generating the set of (normal) files under each directory in Redis
#######

redis-cli EVAL "
    local names = redis.call('KEYS','[^/]*_*');
    for i=1,#names do
        local _,pidx = names[i]:find('.*/');
        local name='';
        if (pidx ~= nil) then
            name=names[i]:sub(0,pidx-1)
        else
            _,pidx = names[i]:find('.*_');
            name=names[i]:sub(0,pidx) .. '/'
        end;
        name='//pf_' .. name;
        redis.call('SADD',name,struct.pack('c0',names[i]));
        redis.call('SADD',KEYS[1],name);
    end
" 1 //snccDirList

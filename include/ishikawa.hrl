-type actor() :: binary().
-type message() :: binary().
-type timestamp() :: vclock:vclock().
-type timestampMatrix() :: mclock:mclock().
-record(state, {actor :: actor(),
                vv :: timestamp(),
                members :: [term()],
                svv :: timestamp(),
                rtm :: timestampMatrix(),
                toBeDlvrdQ :: [{timestamp(), message()}]}).
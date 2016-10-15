-define(PEER_IP, {127,0,0,1}).
-define(PEER_PORT, 9000).

-type actor() :: binary().
-type message() :: binary().
-type timestamp() :: vclock:vclock().
-type timestamp_matrix() :: mclock:mclock().
-type node_spec() :: {node(), inet:ip_address(), non_neg_integer()}.

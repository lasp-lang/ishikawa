-define(APP, ishikawa).
-define(DELIVER_LOCALLY_DEFAULT, false).

-type actor() :: term().
-type message() :: term().
-type timestamp() :: vclock:vclock().
-type timestamp_matrix() :: mclock:mclock().

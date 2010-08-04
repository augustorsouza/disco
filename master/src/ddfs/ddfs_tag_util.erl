-module(ddfs_tag_util).

-include("ddfs_tag.hrl").

-export([check_token/4, encode_tagcontent/1, encode_tagcontent_secure/2,
         decode_tagcontent/1, update_tagcontent/4, validate_urls/1]).

-export([make_tagcontent/6]).

-spec check_token(tokentype(), token(), token(), token()) -> tokentype() | 'false'.
check_token(TokenType, Token, ReadToken, WriteToken) ->
    Auth = fun(Tok, CurrentToken) when Tok =:= CurrentToken -> true;
              (internal, _) -> true;
              (_, null) -> true;
              (_, _) -> false
           end,
    WriteAuth = Auth(Token, WriteToken),
    case TokenType of
        read ->
            case Auth(Token, ReadToken) of
                % It is possible that the Token also carries
                % write-privileges, either because the write-token
                % is not set, or because ReadToken =:= WriteToken.
                true when WriteToken =:= null -> write;
                true when WriteToken =:= ReadToken -> write;
                true -> read;

                % We should not allow reads if the
                % WriteToken check passes just because it
                % hasn't been set.
                false when WriteToken =:= null -> false;
                false when WriteAuth =:= true -> write;
                false -> false
            end;
        write when WriteAuth =:= true -> write;
        write -> false
    end.

-spec make_tagcontent(binary(), binary(), token(), token(),
                      [binary()], user_attr()) -> tagcontent().
make_tagcontent(Id, LastModified, ReadToken, WriteToken, Urls, UserAttr) ->
    #tagcontent{id = Id,
                last_modified = LastModified,
                read_token = ReadToken,
                write_token = WriteToken,
                urls = Urls,
                user = UserAttr}.

-spec encode_tagcontent(tagcontent()) -> binary().
encode_tagcontent(D) ->
    list_to_binary(mochijson2:encode({struct,
        [{<<"version">>, 1},
         {<<"id">>, D#tagcontent.id},
         {<<"last-modified">>, D#tagcontent.last_modified},
         {<<"read-token">>, D#tagcontent.read_token},
         {<<"write-token">>, D#tagcontent.write_token},
         {<<"urls">>, D#tagcontent.urls},
         {<<"user-data">>, {struct, D#tagcontent.user}}
        ]})).

-spec encode_tagcontent_secure(tagcontent(), tokentype()) -> binary().
encode_tagcontent_secure(D, read) ->
    L = [{<<"read-token">>, D#tagcontent.read_token}],
    encode_tagcontent_secure0(D, L);

encode_tagcontent_secure(D, write) ->
    L = [{<<"read-token">>, D#tagcontent.read_token},
         {<<"write-token">>, D#tagcontent.write_token}],
    encode_tagcontent_secure0(D, L).

encode_tagcontent_secure0(D, Tokens) ->
    Struct = [{<<"version">>, 1},
              {<<"id">>, D#tagcontent.id},
              {<<"last-modified">>, D#tagcontent.last_modified},
              {<<"urls">>, D#tagcontent.urls},
              {<<"user-data">>, {struct, D#tagcontent.user}}],
    list_to_binary(mochijson2:encode({struct, Struct ++ Tokens})).

lookup(Key, List) ->
   {value, {_, Value}} = lists:keysearch(Key, 1, List),
   Value.
lookup(Key, Default, List) ->
    proplists:get_value(Key, List, Default).

-spec lookup_tagcontent([{_, _}]) -> tagcontent().
lookup_tagcontent(L) ->
    {struct, UserData} = lookup(<<"user-data">>, {struct, []}, L),
    #tagcontent{id = lookup(<<"id">>, L),
                urls = lookup(<<"urls">>, L),
                last_modified = lookup(<<"last-modified">>, L),
                read_token = lookup(<<"read-token">>, null, L),
                write_token = lookup(<<"write-token">>, null, L),
                user = UserData}.

-spec decode_tagcontent(binary()) ->
                        {'error', corrupted_json | invalid_object} |
                         {'ok', tagcontent()}.
decode_tagcontent(TagData) ->
    case catch mochijson2:decode(TagData) of
        {'EXIT', _} ->
            {error, corrupted_json};
        {struct, Body} ->
            case catch lookup_tagcontent(Body) of
                {'EXIT', _} ->
                    {error, invalid_object};
                TagContent ->
                    {ok, TagContent}
            end
    end.

-spec update_tagcontent(tagname(), attrib(), _, _) -> 
                        {error, invalid_url_object} | {ok, tagcontent()}.
update_tagcontent(TagName, Field, Value, #tagcontent{} = Tag) ->
    Updated = Tag#tagcontent{id = ddfs_util:pack_objname(TagName, now()),
                             last_modified = ddfs_util:format_timestamp()},
    update_tagcontent(Field, Value, Updated);

update_tagcontent(TagName, Field, Value, _Tag) ->
    New = #tagcontent{read_token = null,
                      write_token = null,
                      urls = [],
                      user = []},
    update_tagcontent(TagName, Field, Value, New).

-spec update_tagcontent(attrib(), _, tagcontent()) ->
                       {error, invalid_url_object} | {ok, tagcontent()}.
update_tagcontent(read_token, Token, Tag) ->
    {ok, Tag#tagcontent{read_token = list_to_binary(Token)}};

update_tagcontent(write_token, Token, Tag) ->
    {ok, Tag#tagcontent{write_token = list_to_binary(Token)}};

update_tagcontent(urls, Urls, Tag) ->
    case validate_urls(Urls) of
        true ->
            {ok, Tag#tagcontent{urls = Urls}};
        false ->
            {error, invalid_url_object}
    end;

update_tagcontent({user, Key}, Attr, Tag) ->
    {ok, Tag#tagcontent{user = lists:keystore(Key,
                                              1,
                                              Tag#tagcontent.user,
                                              {Key, Attr})}}.

-spec validate_urls([[_]]) -> bool().
validate_urls(Urls) ->
    [] =:= (catch lists:flatten([[1 || X <- L, not is_binary(X)] || L <- Urls])).

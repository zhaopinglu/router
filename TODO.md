+ Implement keep alive for callback ?
+ Handle errors correctly
+ Fix tests
+ Write new test
+ Should we detect the right protocol for websocket ? Like trying with graphql-ws header and then with the other one to really reject the connection
+ Add auth on subscription callback


BONUS
+ Create a protocol abstraction to automatically handle all the protocol stuffs (server and client side)(including multipart, websocket, callback)

```
enum GraphQLProtocol {
    Callback,
    Multipart,
    Callback,
}

impl Stream
impl Sink
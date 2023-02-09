+ Handle errors correctly
+ Fix tests
+ Write new test
+ Implement passthrough mode
    - Which ws protocol ?
    - Maybe detect the right protocol ?
+ Implement subscription callback with apollo server
+ Add auth on subscription callback
+ Add support for websocket comm ?
+ Create a protocol abstraction to automatically handle all the protocol stuffs (server and client side)(including multipart, websocket, callback)


+ Put all the subscription logic into subgraph_service

```
enum GraphQLProtocol {
    Callback,
    Multipart,
    Callback,
}

impl Stream
impl Sink
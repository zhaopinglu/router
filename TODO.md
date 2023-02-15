+ Add error mesage when connection closed by heartbeat
+ Clean and write MORE tests in notification.rs
+ Handle errors correctly
+ Should we detect the right protocol for websocket ? Like trying with graphql-ws header and then with the other one to really reject the connection
+ Add auth on subscription callback
+ Do we want to make the keep alive timeout configurable ? For now it's 15secs


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
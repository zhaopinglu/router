import "jwt_claims" as my_mod;

// Call map_response with our service and pass in a string with the name
// of the function to callback
fn supergraph_service(service) {
    const request_callback = Fn("process_request");
    const response_callback = Fn("process_response");
    service.map_request(request_callback);
    service.map_response(response_callback);
}

// This will examine our context for any JWT claims that our
// JWT validation has extracted.
//
// We can take actions based on the evaluation of those claims.
//
// In this example we return UNAUTHROISED if there are no claims
// or "iss" claim
// is either:
//  - not present
//  - not equal to "issuer"
//
fn process_request(request) {
    my_mod::process_request(request);
}

fn process_response(response) {
    my_mod::process_response(response);
}

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
    try {
        let claims = request.context[APOLLO_AUTHENTICATION_JWT_CLAIMS];
        if claims == () {
            throw #{
                status: 401,
                message: "No claims presented in this request"
            };
        }
        if !claims.contains("iss") {
            throw #{
                status: 401,
                message: "No issuer presented in this request"
            };
        }
        if claims["iss"] != "https://idp.local" {
            throw #{
                status: 401,
                message: `Issuer ${claims.iss} is not accepted here`
            };
        }
        // We are happy we have claims from the correct idp, do something
        // with them. In this case, we'll just print them out.
        print(`claims: ${claims}`);
    }
    catch(err)
    {
        // log any errors
        log_error(`Error during claims processing: ${err}`);
        throw err;
    }
}

fn process_response(request) {
    try {
        let claims = response.context[APOLLO_AUTHENTICATION_JWT_CLAIMS];
        if claims == () {
            throw #{
                status: 401,
                message: "No claims presented in this response"
            };
        }
        if !claims.contains("iss") {
            throw #{
                status: 401,
                message: "No issuer presented in this response"
            };
        }
        if claims["iss"] != "https://idp.local" {
            throw #{
                status: 401,
                message: `Issuer ${claims.iss} is not accepted here`
            };
        }
        // We are happy we have claims from the correct idp, do something
        // with them. In this case, we'll just print them out.
        print(`claims: ${claims}`);
    }
    catch(err)
    {
        // log any errors
        log_error(`Error during claims processing: ${err}`);
        throw err;
    }
}

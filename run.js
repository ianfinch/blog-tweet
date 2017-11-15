var lambda = require("./lambda.js");

function callbackHandler(err, success) {

    if (err) {
        console.error(err);
    } else {
        console.info(success);
    }

}

lambda.handler(null, null, callbackHandler);

var https = require('https');

var APP_ID = 'amzn1.echo-sdk-ams.app.31f81f3c-4b1b-44c7-8126-cd4e468b72c2';

exports.handler = function (event, context, callback) {
    if (event.session.application.applicationId !== APP_ID) {
        callback(new Error('Invalid Application ID'), null);
        return;
    }
    console.log(event.request);
    if (!event.session.user.accessToken) {
        callback(null, {
            version: '1.0',
            response: {
                outputSpeech: {type: 'PlainText', text: 'Please go to your Alexa app and link your account.'},
                card: {type: 'LinkAccount'}
            }
        });
        return;
    }
    var json = JSON.stringify(event);
    var options = {
        method: 'POST',
        hostname: 'api.rogertalk.com',
        path: '/ask/v1/request',
        headers: {
            'Content-Type': 'application/json;charset=UTF-8',
            'Content-Length': Buffer.byteLength(json)
        }
    };
    var req = https.request(options, function (res) {
        var body = '', calledBack = false;
        res.on('error', function (e) {
            if (calledBack) return;
            calledBack = true;
            callback(e, null);
        });
        res.on('data', function (chunk) {
            if (calledBack) return;
            body += chunk;
        });
        res.on('end', function () {
            if (calledBack) return;
            calledBack = true;
            callback(null, JSON.parse(body));
            body = null;
        });
    });
    req.on('error', function (e) {
        context.fail('Error: ' + e.message);
    });
    req.write(json);
    req.end();
};

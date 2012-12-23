var WebSocketServer = require('ws').Server
  , wss = new WebSocketServer({port: process.env.OPENSHIFT_NODEJS_PORT });
wss.on('connection', function(ws) {
    ws.on('message', function(message) {
        console.log('received: %s', message);
    });
    ws.send('something');
});
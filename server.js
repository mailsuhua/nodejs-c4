
/**
 * New node file
 */
var http = require('http');
var url = require("url");
var fs = require('fs');
var net = require('net');
var HashMap = require('./hashmap.js').HashMap;
var ev = require('./event.js');

var VERSION="0.20.0"

var userMap = new HashMap();

function newSession(user, hash){
   var obj = new Object();
   obj.remoteHost = '';
   obj.remotePort = 80;
   obj.sid = hash;
   obj.user = user;
   obj.socket = null;
   obj.sequence = 0;
   obj.cachedEvents = [];
   obj.closed = false;
   obj.passiveread = true;
   obj.closecb = null;
   obj.close = function() {
      if(null != obj.socket){
        obj.socket.destroy();
        obj.socket = null;
      }
      obj.closed = true;
      if(userMap.has(obj.user)){
       var userSessions = userMap.get(obj.user);
       userSessions.remove(obj.sid);
      }
      obj.cachedEvents= [];
      if(obj.closecb != null){
        obj.closecb(obj);
      }
   }

   obj.endWriter = function(){
      if(null != obj.writer && obj.passiveread){
        obj.writer.end();
        obj.writer = null;
      }
      if(null != obj.socket && obj.passiveread){
        obj.socket.pause();
      }
   }

   obj.checkCachedData = function(){
      if(null == obj.writer || obj.closed)
      {
        return;
      }
      for(var i = 0; i < obj.cachedEvents.length; i++){
          if(!obj.writer.write(obj.cachedEvents[i])){
             console.log("##########Busy again@@");
          }
      }
      obj.cachedEvents= [];
      if(obj.socket != null)
      {
        obj.socket.resume();
      }
    };

   return obj;
}



function getCreateSession(user, hash, writer, passiveread){
  if(!userMap.has(user)){
    userMap.set(user, new HashMap());
  }
  var userSessions = userMap.get(user);
  if(!userSessions.has(hash)){
    userSessions.set(hash, newSession(user, hash));
  }
  var session = userSessions.get(hash);
  if(null != writer)
  {
      session.writer = writer;
  }
  session.passiveread = passiveread;
  return session;
}

function onIndex(request, response) {
   fs.readFile('./index.html', function (err, html) {
    if (err) {
        throw err; 
    }       
    response.writeHead(200, {"Content-Type": "text/html"});
    response.write(html.toString().replace("${version}",VERSION).replace("${version}",VERSION));
    response.end()
});
}


var HTTP_REQUEST_EVENT_TYPE=1000;
var ENCRYPT_EVENT_TYPE=1501;
var EVENT_TCP_CONNECTION_TYPE = 12000;
var EVENT_USER_LOGIN_TYPE=12002;
var EVENT_TCP_CHUNK_TYPE=12001;
var EVENT_SOCKET_READ_TYPE=13000;

var TCP_CONN_OPENED  = 1;
var TCP_CONN_CLOSED  = 2;

var ENCRYPTER_NONE  = 0;
var ENCRYPTER_SE1  = 1;



function handleEvent(evv, user, writer, passiveread, allsessions){
  //console.log("Session[" + evv.hash + "] handle event:" + evv.type);
  switch(evv.type){
    case EVENT_USER_LOGIN_TYPE:
    {
      if(userMap.has(user)){
         userMap.get(user).forEach(function(sess, hash) {
           sess.close();
        });
         userMap.get(user).clear();
      }
      userMap.remove(user);
      return null;
    }
    case HTTP_REQUEST_EVENT_TYPE:
    {
      //var writer = ispull?response:null;
      var session = getCreateSession(user, evv.hash, writer, passiveread);
      currentSession = session;
      var host = evv.host;
      var port = 80;
      if(evv.method.toLowerCase() == 'connect'){
        port = 443;
      }
      var ss = host.split(":");
        if(ss.length == 2){
          host = ss[0];
          port = parseInt(ss[1]);
      }

      if(null != session.socket && session.remoteHost == host && session.remotePort == port){
        session.socket.write(evv.rawContent);
        return;
      }
      session.remoteHost = host;
      session.remotePort = port;
      if(null != session.socket){
        session.socket.destroy();
      }
      var remoteaddr = host+":" + port;
      var client = net.connect(port, host ,  function() { 
        console.log("####Connected:" + remoteaddr + " for hash:" + evv.hash);
        session.socket = client;
        if(passiveread){
          session.socket.pause();
        }
        session.sequence = 0;
        if(evv.method.toLowerCase() == 'connect'){
          var established = ev.newEvent(EVENT_TCP_CHUNK_TYPE, 1, session.sid);
          established.seq = session.sequence++;
          established.content=new Buffer("HTTP/1.1 200 OK\r\n\r\n");
          if(null != session.writer)
          {
            session.writer.write(ev.encodeChunkTcpChunkEvent(established));
          }else{
            session.cachedEvents.push(ev.encodeChunkTcpChunkEvent(established));
          }
        }else{
          session.socket.write(evv.rawContent);
          //console.log("####writed:" + evv.rawContent.toString());
        }       
      });
      client.on('data', function(data) {
        var chunk = ev.newEvent(EVENT_TCP_CHUNK_TYPE, 1, session.sid);
        chunk.seq = session.sequence++;
        chunk.content=data;   
        //console.log("###[" + session.sid + "]recv chunk:" + data.length);
        if(null != session.writer){
          session.checkCachedData();
          if(!session.writer.write(ev.encodeChunkTcpChunkEvent(chunk))){
            session.socket.pause();
            //console.log("###[" + session.sid + "]pause again");
          }
        }else{
          client.pause();
          session.cachedEvents.push(ev.encodeChunkTcpChunkEvent(chunk));
          console.log("###Invalid situataion that response writer is null.");
        }
      });
      client.on('end', function() {
                          
      });
      client.on('error', function(err) {
        console.log("####Failed to connect:" + remoteaddr + " :" + err);           
      });
      client.on('close', function(had_error) {
        if(remoteaddr == (session.remoteHost + ":" + session.remotePort) && !session.closed){
            console.log("####Close connection for " + remoteaddr);    
            var closed = ev.newEvent(EVENT_TCP_CONNECTION_TYPE, 1, session.sid);  
            closed.status =  TCP_CONN_CLOSED;
            closed.addr =  remoteaddr;
            if(null != session.writer){
              session.writer.write(ev.encodeChunkConnectionEvent(closed));
            }else{
              //cache this event.
              session.cachedEvents.push(ev.encodeChunkConnectionEvent(closed));
            } 
            session.endWriter();
            session.close();        
        }
      });
      return session;
    }
    case EVENT_TCP_CONNECTION_TYPE:
    {
      var session = getCreateSession(user, evv.hash, writer, passiveread);
      currentSession = session;
      if(evv.status == TCP_CONN_CLOSED){
        session.close();
      }
      return session;
    }
    case EVENT_TCP_CHUNK_TYPE:
    {
      var session = getCreateSession(user, evv.hash, writer, passiveread);
      currentSession = session;
      if(null == session.socket){
        session.close();
        return;
      }
      session.socket.write(evv.content);
      return session;
    }
    case EVENT_SOCKET_READ_TYPE:
    {
      if(evv.hash == 0 && null != allsessions){
        allsessions.forEach(function(sess, hash) {
           sess.checkCachedData();
        });
        return;
      }
      var session = getCreateSession(user, evv.hash, writer, passiveread);
      var check = function(){
        for(var i = 0; i < session.cachedEvents.length; i++){
          writer.write(session.cachedEvents[i]);
        }
        session.cachedEvents= [];
        if(session.closed){
          session.endWriter();
          return;
        }
        if(session.writer == null){
          return;
        }
        if(session.socket != null)
        {
          session.socket.resume();
        }else{
          setTimeout(check, 1);
        }
    };
    check();
    return session;
  }
  default:
  {
    console.log("################Unsupported type:" + evv.type);
    break;
  }
 }
  return null;
}

function now(){
  return Math.round((new Date()).getTime()/ 1000);
}

function onInvoke(request, response) {
   var length = parseInt(request.headers['content-length']);
   var user = request.headers['usertoken'];
   var ispull = (url.parse(request.url).pathname == '/pull');
   var c4miscinfo = request.headers['c4miscinfo'];
   if(c4miscinfo != null){
    var miscInfo = c4miscinfo.split('_');
    var timeout = parseInt(miscInfo[0]);
    var maxread = parseInt(miscInfo[1]);
   }
   
   var postData = new Buffer(length);
   var recvlen = 0;
   var responsed = false;
   var startTime = now();

   var currentSession = null;

   if(ispull){
       setTimeout(function(){
       if(null != currentSession && currentSession.writer != null){
         currentSession.endWriter();
       }else{
        response.end();
       }
      }, timeout*1000);
   }


   response.writeHead(200, {"Content-Type": "image/jpeg",  "C4LenHeader":1, "Connection":"keep-alive"});
   request.addListener("data", function(chunk) {
      chunk.copy(postData, recvlen, 0);
      recvlen += chunk.length;
    });

   response.on('drain', function () {
      if(null != currentSession && null != currentSession.socket && null != currentSession.writer){
        currentSession.socket.resume();
      }
   });

   request.addListener("end", function() {
      if(recvlen == length && length > 0){
        var readBuf = ev.newReadBuffer(postData);
        var events = ev.decodeEvents(readBuf);
        //console.log("Total events is "+events.length);
        for (var i = 0; i < events.length; i++)
        {
            var evv = events[i];
            //console.log("Decode event " + evv.type + ":" + evv.version + ":" + evv.hash);
            var writer = response;
            if(!ispull){
               writer = null;
            }
            currentSession = handleEvent(evv, user, writer, true, null);
        }
      }else{
        console.log("Request not full data ");
      }
      if(!ispull){
        response.end();
      }
   });

}

var handle = {}
handle["/"] = onIndex;
handle["/pull"] = onInvoke;
handle["/push"] = onInvoke;

function route(pathname, request, response) {
  if (typeof handle[pathname] === 'function') {
    handle[pathname](request, response);
  } else {
    response.writeHead(404, {"Content-Type": "text/plain"});
    response.end();
  }
}

function onRequest(request, response) {
  var pathname = url.parse(request.url).pathname;
  route(pathname, request, response)
}

var ipaddr  = process.env.OPENSHIFT_NODEJS_IP || "0.0.0.0";
var port = process.env.VCAP_APP_PORT ||process.env.OPENSHIFT_NODEJS_PORT || process.env.PORT || 8080;
var server = http.createServer(onRequest);
server.listen(port, ipaddr);
console.log('Server running at '+ ipaddr + ":" + port);

var userConnTable = new HashMap();
var userConnBufTable = new HashMap();

function getUserConnSessionGroup(user, index){
  if(!userConnTable.has(user)){
     userConnTable.set(user, new HashMap());
  }
  var sessions = userConnTable.get(user);
  if(!sessions.has(index)){
    sessions.set(index, new HashMap());
  }
  return sessions.get(index);
}

function getUserConnBuffer(user, index){
  if(!userConnBufTable.has(user)){
     userConnBufTable.set(user, new HashMap());
  }
  var bufs = userConnBufTable.get(user);
  if(!bufs.has(index)){
    bufs.set(index, new Buffer(0));
  }
  return bufs.get(index);
}

function resumeCachedConn(ss, writer){
  var deleteSids = [];
  ss.forEach(function(session, sid) {
    if(null != session && null != session.socket  && !session.closed){
      session.writer = writer;
      session.socket.resume();
      session.checkCachedData();
      //console.log('########resume session:' + sess.sid);
    }else{
      deleteSids.push(session.sid);
    }
  });
  for (var i = 0; i < deleteSids.length; i++)
  {
    ss.remove(deleteSids[i]);
  }
}

server.on('upgrade', function(req, connection, head) {
    var user = req.headers['usertoken'];
    var localConn = connection;
    var index = req.headers['connectionindex'];
    console.log('Websocket establis with index:' + index);
    localConn.write(
        'HTTP/1.1 101 Web Socket Protocol Handshake\r\n' + 
        'Upgrade: WebSocket\r\n' + 
        'Connection: Upgrade\r\n' +
        '\r\n'
    );
    var cumulateBuf = getUserConnBuffer(user, index);
    var chunkLen = -1;
    var sesss = getUserConnSessionGroup(user, index);
    resumeCachedConn(sesss, localConn);

    //for (var i = 0; i < sesss.length; i++)
    //{
    //  var sess = sesss[i];
    //  sess.checkCachedData();
    //}

    //userConnTable.get(user).set(index, sesss);
    localConn.on("data", function(data) {
      if(null == cumulateBuf || cumulateBuf.length == 0){
        cumulateBuf = data;
      }else{
        cumulateBuf = Buffer.concat([cumulateBuf, data])
      }
      for(;;){
        if(chunkLen == -1){
           if(cumulateBuf.length >= 4){
              var tmplen = cumulateBuf.length;
              chunkLen = cumulateBuf.readInt32BE(0);
              cumulateBuf = cumulateBuf.slice(4, tmplen);
           }else{
              return;
           }
        }
        if(chunkLen > 0){
          if(cumulateBuf.length >= chunkLen){
            var chunk = new Buffer(chunkLen);
            cumulateBuf.copy(chunk, 0, 0, chunkLen);
            var tmplen = cumulateBuf.length;
            cumulateBuf = cumulateBuf.slice(chunkLen, tmplen);
            chunkLen = -1;
            var readBuf = ev.newReadBuffer(chunk);
            var sess = handleEvent(ev.decodeEvent(readBuf), user, localConn, false, sesss);
            if(null != sess){
              sesss.set(sess.sid, sess);
              sess.closecb = function(session){
                sesss.remove(session.sid);
                //console.log("####Sessions size:" + sesss.count());
              };
            }
          }else{
            return;
          }
        }
      }
    });
    localConn.on("end", function() {

    });
    localConn.on("close", function() {
      //console.log('########websocket closed');
     sesss.forEach(function(session, sid) {
        if(null != session && null != session.socket  && !session.closed){
          session.socket.pause();
        }
      });
      userConnBufTable.get(user).set(index, cumulateBuf);
    });
    localConn.on("error", function() {

    });

    localConn.on("drain", function() {
      resumeCachedConn(sesss, localConn);
    });
});
self.addEventListener("install",function(){self.skipWaiting()});
self.addEventListener("activate",function(e){e.waitUntil(self.clients.claim())});
self.addEventListener("fetch",function(){});
self.addEventListener("push",function(e){
  var n=0;try{n=(e.data&&e.data.json().n)||0}catch(x){}
  e.waitUntil((async function(){
    var list=await self.clients.matchAll({type:"window",includeUncontrolled:true});
    if(n&&self.navigator&&self.navigator.setAppBadge){try{await self.navigator.setAppBadge(n)}catch(x){}}
    if(list.some(function(c){return c.visibilityState==="visible"}))return;
    await self.registration.showNotification("Yeni mesajın var!",{body:"",tag:"msg",renotify:true,icon:"icon-192.png",badge:"icon-192.png"});
  })());
});
self.addEventListener("notificationclick",function(e){
  e.notification.close();
  e.waitUntil(self.clients.matchAll({type:"window",includeUncontrolled:true}).then(function(l){
    for(var i=0;i<l.length;i++){if("focus" in l[i])return l[i].focus()}
    if(self.clients.openWindow)return self.clients.openWindow("/");
  }));
});

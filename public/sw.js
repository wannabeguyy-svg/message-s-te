self.addEventListener("install",function(){self.skipWaiting()});
self.addEventListener("activate",function(e){e.waitUntil(self.clients.claim())});
self.addEventListener("fetch",function(){});
self.addEventListener("push",function(e){
  var d={};try{d=e.data?e.data.json():{}}catch(x){}
  var n=d.n||0,q=!!d.q;
  e.waitUntil((async function(){
    var list=await self.clients.matchAll({type:"window",includeUncontrolled:true});
    if(n&&self.navigator&&self.navigator.setAppBadge){try{await self.navigator.setAppBadge(n)}catch(x){}}
    if(list.some(function(c){return c.visibilityState==="visible"}))return;
    // Chrome her itme bildiriminde bir bildirim göstermeyi şart koşar; sessiz modda kısa süre sonra kapatılır, rakam kalır
    await self.registration.showNotification("Yeni mesajın var!",{body:"",tag:"msg",renotify:!q,silent:q,icon:"icon-192.png",badge:"icon-192.png"});
    if(q){
      await new Promise(function(r){setTimeout(r,1500)});
      var ns=await self.registration.getNotifications({tag:"msg"});
      ns.forEach(function(x){x.close()});
    }
  })());
});
self.addEventListener("notificationclick",function(e){
  e.notification.close();
  e.waitUntil(self.clients.matchAll({type:"window",includeUncontrolled:true}).then(function(l){
    for(var i=0;i<l.length;i++){if("focus" in l[i])return l[i].focus()}
    if(self.clients.openWindow)return self.clients.openWindow("/");
  }));
});

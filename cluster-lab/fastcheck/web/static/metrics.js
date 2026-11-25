// A display percentage must never imply completion while work remains.
function progressPercentage(processed,total){
  if(total<=0)return 0;
  if(processed>=total)return 100;
  return Math.floor(Math.max(0,processed)*1000/total)/10;
}
function devicePoolMetrics(state){
  const devices=state.progress?.devices,settings=state.settings||{};
  const metadata=state.summary?.device_pool||{};
  return {hasSnapshot:!!devices,
    isGsa:state.protocol==='gsa'||settings.protocol==='gsa'||!!devices,
    ready:devices?.ready||0,registered:devices?.total||0,
    target:devices?.target??settings.device_capacity??devices?.total??0,
    warmup:devices?.warmup??settings.warmup_devices??metadata.warmup,
    restored:devices?.restored??metadata.restored,
    created:devices?.registered_this_run??metadata.registered_this_run,
    idle:devices?.idle||0,resting:devices?.resting||0,draining:devices?.draining||0};
}
if(typeof module!=='undefined')module.exports={progressPercentage,devicePoolMetrics};

const $=id=>document.getElementById(id);let csrf='',job=null,timer=null,offset=0,totalRows=0,lastCount=-1,watchGeneration=0,resultGeneration=0;
const names={valid:'密正',need_2fa:'双重验证',wrong_password:'密码错误',security_lock:'安全锁定',locked:'锁定',init_refused:'初始化拒绝',credentials_rejected:'信息被拒绝',error:'请求失败',unknown:'未判断',not_started:'未开始',stopped:'已停止',needs_setup:'需后续设置'};
const numeric=['processes','threads','proxy_target','proxy_ttl','timeout','device_capacity','device_rest_after','device_rest_seconds'];
let defaults={};
function notice(text){$('message').textContent=text;$('message').hidden=false;setTimeout(()=>$('message').hidden=true,5500)}
async function api(path,options={}){options.headers={...(options.headers||{}),'X-CSRF-Token':csrf};const r=await fetch(path,options);const data=await r.json();if(!r.ok)throw new Error(data.error||`请求失败 ${r.status}`);return data}
function jsonPost(path,data){return api(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)})}
function readSettings(){const s={...defaults};numeric.forEach(k=>s[k]=Number($(k).value));s.protocol=$('protocol').value;s.device_selection=$('device_selection').value;s.proxy_scheme=$('proxy_scheme').value;s.max_attempts=Number($('retries').value)+1;s.debug=$('debug').checked;return s}
function fillSettings(s){defaults={...s};numeric.forEach(k=>{if(s[k]!==undefined)$(k).value=s[k]});$('protocol').value=s.protocol||'web';$('device_selection').value=s.device_selection||'prefer_new';$('proxy_scheme').value=s.proxy_scheme||'http';$('retries').value=Math.max(0,(s.max_attempts||1)-1);$('debug').checked=!!s.debug;$('device-options').hidden=$('protocol').value!=='gsa'}
function cell(row,text,className){const e=document.createElement('td');e.textContent=text??'—';if(className)e.className=className;row.append(e);return e}
async function results(){if(!job?.id)return;const id=job.id,generation=++resultGeneration,count=job.progress?.counts?.processed;const d=await api(`/api/jobs/${id}/results?category=${$('filter').value}&offset=${offset}&limit=100`);if(job?.id!==id||generation!==resultGeneration)return;lastCount=count;totalRows=d.total;$('results').replaceChildren();d.rows.forEach(v=>{const row=document.createElement('tr');cell(row,v.row);cell(row,v.email);const c=cell(row,'');const tag=document.createElement('span');tag.className='badge '+(['valid','need_2fa'].includes(v.status)?'good':v.status==='error'?'bad':'');tag.textContent=names[v.status]||v.status;c.append(tag);cell(row,v.stage);cell(row,v.http);cell(row,v.apple_ec);cell(row,v.device);cell(row,v.total_ms===undefined?'—':(v.total_ms/1000).toFixed(2)+'s');cell(row,Math.max(0,(v.attempts||1)-1));$('results').append(row)});if(!d.rows.length){const r=document.createElement('tr');const c=cell(r,'暂无符合条件的结果','empty');c.colSpan=9;$('results').append(r)}$('page').textContent=`${d.total} 条结果 · 第 ${Math.floor(offset/100)+1} 页`;$('prev').disabled=offset===0;$('next').disabled=offset+100>=d.total}
function render(state){job=state;const p=state.progress||{},c=p.counts||{total:state.accounts||0,processed:0,remaining:state.accounts||0,active:0};const fields=[['总数','total'],['完成','processed'],['剩余','remaining'],['当前校验','active'],['密正','valid'],['密码错误','wrong_password'],['双重验证','need_2fa'],['锁定','locked'],['超时','timeout'],['未判断','undetermined']];$('stats').replaceChildren();for(const[label,key]of fields){const box=document.createElement('div');box.className='stat';box.textContent=label;const n=document.createElement('strong');n.textContent=(c[key]||0).toLocaleString();box.append(n);$('stats').append(box)}const active=!['completed','failed','stopped','unknown'].includes(state.state);$('state').textContent=(p.phase||({completed:'已完成',running:'运行中',queued:'准备中',stopped:'已停止',failed:'启动失败',unknown:'结果未知'}[state.state]||state.state));if(!state.id)$('state').textContent='未开始';$('start').disabled=active;$('stop').disabled=!active;$('export').disabled=!state.id;$('logs').disabled=!state.id;const percentage=progressPercentage(c.processed,c.total);$('progress').value=percentage;$('percent').textContent=percentage+'%';$('progress-text').textContent=`${c.processed||0} / ${c.total||0}`;const elapsed=p.elapsed_seconds||state.summary?.elapsed_seconds||0;$('runtime').textContent=`用时 ${elapsed.toFixed(1)} 秒 · ${elapsed?Math.round((c.processed||0)*3600/elapsed).toLocaleString():0} 条/小时`;$('runtime').title='按已处理任务数除以累计运行时长计算，包含错误、未判断和停止后的中断记录，不是登录成功速度。';$('capacity').textContent=state.settings?`认证工作进程 ${state.settings.processes} × 每进程线程 ${state.settings.threads} · 容量 ${state.settings.processes*state.settings.threads}`:'';renderDevices(state);if(lastCount!==c.processed){results().catch(e=>notice(e.message))}if(state.state==='failed')notice('任务未启动或执行失败：'+(state.error_type||'请查看日志'));if(!active&&timer){clearTimeout(timer);timer=null;history().catch(()=>{})}}
function renderDevices(state){
  const p=state.progress||{},d=p.devices;
  const terminal=['completed','failed','stopped','unknown'].includes(state.state);
  const m=devicePoolMetrics(state);
  let deviceText='';
  if(m.isGsa){
    // 只露"就绪 x/y"：注册进度、空闲数、休息数、门槛都属于内部策略，不必展示。
    deviceText=m.hasSnapshot?`就绪 ${m.ready}/${m.target}`:'身份池准备中';
  }
  $('devices').textContent=deviceText;
  const usage=p.device_usage||state.summary?.device_usage,box=$('device-report');
  box.hidden=!usage;
  if(!usage||!box.open)return;
  $('device-rows').replaceChildren();
  for(const [id,value] of Object.entries(usage)){
    const row=document.createElement('tr');
    cell(row,id);cell(row,value.total_uses);cell(row,value.in_flight||0);
    cell(row,value.draining?'等待在途结束后休息':value.rest_remaining_seconds>0?`休息 ${value.rest_remaining_seconds.toFixed(1)} 秒`:value.in_flight?'执行中':'空闲');
    $('device-rows').append(row);
  }
}
async function watch(id){
  const generation=++watchGeneration;
  if(timer)clearTimeout(timer);
  timer=null;offset=0;lastCount=-1;++resultGeneration;
  async function update(){
    if(generation!==watchGeneration)return;
    try{
      const state=await api('/api/jobs/'+id);
      if(generation!==watchGeneration)return;
      render(state);
      if(['completed','failed','stopped','unknown'].includes(state.state)){
        await results();await history();return;
      }
    }catch(e){notice(e.message)}
    if(generation===watchGeneration)timer=setTimeout(update,1000);
  }
  await update();
}
async function history(){const d=await api('/api/jobs');$('history').replaceChildren();if(!d.jobs.length){$('history').textContent='暂无任务';return}d.jobs.forEach(v=>{const row=document.createElement('div');row.className='history-row';const text=document.createElement('span');text.textContent=`${v.created_at.replace('T',' ')} · ${v.protocol.toUpperCase()} · ${v.accounts} 个 · ${v.simulation?'演示':v.state}`;const b=document.createElement('button');b.textContent='查看';b.onclick=()=>watch(v.id);row.append(text,b);$('history').append(row)});return d.jobs}
async function load(){const d=await api('/api/settings');fillSettings(d.settings);$('proxy-saved').textContent=d.proxy_saved?'代理 API 已保存在服务器，留空即可沿用。':'尚未保存代理 API。';const list=await history();if(list?.length)await watch(list[0].id);else render({state:'completed',accounts:0})}
async function start(simulation){const body=new FormData();body.append('settings',JSON.stringify(readSettings()));body.append('simulation',String(simulation));body.append('api_url',$('api-url').value);body.append('proxy_auth',$('proxy-auth').value);if(!simulation){const f=$('accounts-file').files[0];if(f)body.append('accounts',f);else body.append('accounts_text',$('accounts-text').value)}$('start').disabled=true;try{const state=await api('/api/jobs',{method:'POST',body});$('api-url').value='';$('proxy-auth').value='';$('accounts-text').value='';await watch(state.id);await history()}catch(e){notice(e.message);$('start').disabled=false}}
$('device-report').ontoggle=()=>{if(job)renderDevices(job)};
$('protocol').onchange=()=>$('device-options').hidden=$('protocol').value!=='gsa';$('task-form').onsubmit=e=>{e.preventDefault();start(false)};
$('login-button').onclick=async()=>{try{const d=await jsonPost('/api/login',{username:$('username').value,password:$('password').value});csrf=d.csrf;$('password').value='';$('login').hidden=true;$('console').hidden=false;$('logout').hidden=false;await load()}catch(e){notice(e.message)}};$('password').onkeydown=e=>{if(e.key==='Enter')$('login-button').click()};
$('logout').onclick=async()=>{await jsonPost('/api/logout',{});location.reload()};$('save').onclick=async()=>{try{await jsonPost('/api/settings',{settings:readSettings(),api_url:$('api-url').value});$('api-url').value='';notice('配置已保存到服务器');$('proxy-saved').textContent='代理 API 已保存时，留空沿用。'}catch(e){notice(e.message)}};
$('stop').onclick=async()=>{if(job){await jsonPost(`/api/jobs/${job.id}/stop`,{});notice('已请求停止，正在回收在途任务。')}};$('filter').onchange=()=>{offset=0;results()};$('prev').onclick=()=>{offset=Math.max(0,offset-100);results()};$('next').onclick=()=>{offset+=100;results()};let buildTimer=null;
function renderBuild(s){const box=$('build-status');if(!s)return;
  if(s.running){box.textContent=`就绪 ${s.ready_now}/${s.capacity}`;
    $('build-devices').disabled=true;$('stop-build').hidden=false;}
  else{box.textContent=`就绪 ${s.ready_now}/${s.capacity}`;
    $('build-devices').disabled=false;$('stop-build').hidden=true;
    if(buildTimer){clearInterval(buildTimer);buildTimer=null}}}
async function pollBuild(){try{const d=await api('/api/devices/build');renderBuild(d);
  if(!d.running&&buildTimer){clearInterval(buildTimer);buildTimer=null}}catch(e){}}
$('build-devices').onclick=async()=>{
  $('build-devices').disabled=true;
  try{const d=await jsonPost('/api/devices/build',{api_url:$('api-url').value,
      proxy_auth:$('proxy-auth').value,settings:readSettings()});
    renderBuild(d);
    if(buildTimer)clearInterval(buildTimer);
    buildTimer=setInterval(pollBuild,1500);
  }catch(e){notice(e.message);$('build-devices').disabled=false}};
$('stop-build').onclick=async()=>{try{await jsonPost('/api/devices/build/cancel',{});notice('已请求停止')}catch(e){notice(e.message)}};
$('check-proxy').onclick=async()=>{const box=$('proxy-check-result');box.textContent='验证中…';try{const d=await jsonPost('/api/proxy/check',{api_url:$('api-url').value,proxy_auth:$('proxy-auth').value,settings:readSettings()});box.textContent=`✅ 出口 ${d.exit_ip} · 提取 ${d.fetch_ms}ms · 请求 ${d.request_ms}ms`;}catch(e){box.textContent='❌ '+e.message}};$('refresh-history').onclick=()=>history();$('clear-history').onclick=async()=>{if(!confirm('清空全部历史任务？任务目录、分类导出（含邮箱密码）和日志会一起删除，不能恢复。'))return;try{const r=await jsonPost('/api/jobs/clear',{});notice(`已清空 ${r.removed} 个历史任务`);job=null;await history()}catch(e){notice(e.message)}};$('export').onclick=()=>{if(job)location.href=`/api/jobs/${job.id}/exports/${$('export-category').value}`};$('logs').onclick=()=>{if(job)location.href=`/api/jobs/${job.id}/logs`};
(async()=>{try{const d=await api('/api/session');csrf=d.csrf;$('version').textContent='v'+d.version;$('connection').textContent='后台：已连接';$('login').hidden=d.authenticated;$('console').hidden=!d.authenticated;$('logout').hidden=!d.authenticated;if(d.authenticated)await load()}catch(e){$('connection').textContent='后台：连接失败';notice(e.message)}})();

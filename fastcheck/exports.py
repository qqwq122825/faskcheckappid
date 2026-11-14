"""Private account exports; kept separate from credential-free diagnostic logs."""
from collections import Counter
import json
import os
from pathlib import Path

from fastcheck.devices import private_directory
from fastcheck.status import result_bucket

FILES={'valid':'password_ok.txt','wrong_password':'wrong_password.txt','need_2fa':'need_2fa.txt',
       'locked':'locked.txt','timeout':'timeout.txt','invalid':'invalid.txt','undetermined':'undetermined.txt',
       'not_started':'not_started.txt','accepted_passwords':'accepted_passwords.txt'}
LABELS={'valid':'密正','wrong_password':'密码错误','need_2fa':'需要双重验证','locked':'锁定',
        'timeout':'超时','invalid':'已失效','undetermined':'其他未验证','not_started':'未开始','accepted_passwords':'密码已通过（含双重验证）'}


class ClassifiedExports:
    def __init__(self,directory):
        self.directory=private_directory(Path(directory))
        self.counts=Counter()
        for filename in (*FILES.values(),'undetermined-details.jsonl'):
            fd=os.open(self.directory/filename,os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o600)
            os.close(fd)

    def append(self,filename,text):
        fd=os.open(self.directory/filename,os.O_WRONLY|os.O_APPEND)
        with os.fdopen(fd,'a',encoding='utf-8') as handle:handle.write(text+'\n')

    def write(self,account,result):
        bucket='not_started' if result.get('attempts',1)==0 else result_bucket(result)
        pair=account.email+'----'+account.password
        self.append(FILES[bucket],pair)
        self.counts[bucket]+=1
        if bucket in ('valid','need_2fa'):
            self.append(FILES['accepted_passwords'],pair)
            self.counts['accepted_passwords']+=1
        if bucket in ('invalid','undetermined','timeout'):
            safe={key:result.get(key) for key in ('row','protocol','status','kind','stage','http','apple_ec','error_type','total_ms','attempts','history')}
            self.append('undetermined-details.jsonl',json.dumps(safe,ensure_ascii=False,separators=(',',':')))

    def summary(self):
        return {key:{'label':LABELS[key],'file':filename,'accounts':self.counts[key]} for key,filename in FILES.items()}

"""Mutually exclusive result counts and a one-second terminal status panel."""
import shutil
import sys
import time
import unicodedata


def result_bucket(result):
    status = result.get('status')
    if result.get('kind') == 'timeout' or status == 'timeout':
        return 'timeout'
    if status == 'need_2fa':
        return 'need_2fa'
    if status == 'valid' or (status == 'needs_setup' and result.get('password_verified') is True):
        return 'valid'
    if status == 'wrong_password':
        return 'wrong_password'
    if status in ('security_lock', 'locked'):
        return 'locked'
    # -20970 在 init 阶段没有完成密码证明。新任务可开启网页协议
    # 复检；未开启或复检未得到明确结果时，不得猜测为已失效。
    if (status == 'init_refused' or result.get('stage') == 'init') \
            and result.get('apple_ec') == -20970:
        return 'undetermined'
    return 'undetermined'


def snapshot(total, results, active=0):
    counts = {key: 0 for key in ('valid','wrong_password','need_2fa','locked','timeout','invalid','undetermined')}
    processed = 0
    for result in results.values():
        if result.get('attempts', 1) <= 0 or result.get('status') == 'not_started':
            continue
        processed += 1
        counts[result_bucket(result)] += 1
    return dict(counts, total=total, processed=processed, remaining=max(0,total-processed), active=active)


def status_tokens(counts, elapsed, phase, devices=None):
    seconds = max(0,int(elapsed))
    duration = f'{seconds//3600:02}:{seconds//60%60:02}:{seconds%60:02}'
    speed = round(counts['processed']*3600/elapsed) if elapsed>0 else 0
    tokens = [f'[{phase}]',f'总数:{counts["total"]}',f'已运行:{counts["processed"]}',
              f'剩余:{counts["remaining"]}',f'运行中:{counts["active"]}',
              f'密正:{counts["valid"]}',f'错误:{counts["wrong_password"]}',
              f'双重:{counts["need_2fa"]}',f'锁定:{counts["locked"]}',
              f'超时:{counts["timeout"]}',f'已失效:{counts["invalid"]}',
              f'其他未验证:{counts["undetermined"]}',
              f'用时:{duration}',f'速度:{speed}/时']
    if devices is not None:
        if isinstance(devices,dict):
            tokens.extend((f'就绪设备:{devices["ready"]}/{devices["total"]}',
                           f'空闲设备:{devices["idle"]}',f'休息设备:{devices["resting"]}'))
        else:
            tokens.append(f'设备:{devices}')
    return tokens


def wrap_tokens(tokens, width):
    lines, current, used = [], [], 0
    for token in tokens:
        size = sum(2 if unicodedata.east_asian_width(c) in ('W','F') else 1 for c in token)
        if current and used+1+size > width:
            lines.append(' '.join(current))
            current, used = [], 0
        used += size+(1 if current else 0)
        current.append(token)
    if current:
        lines.append(' '.join(current))
    return lines


class StatusPanel:
    def __init__(self, stream=None, clock=time.monotonic):
        self.stream = stream if stream is not None else sys.stdout
        self.clock = clock
        self.last = None
        self.previous_rows = 0

    def update(self, counts, elapsed, phase, *, devices=None, final=False):
        now = self.clock()
        if not final and self.last is not None and now-self.last < 1:
            return False
        tokens = status_tokens(counts,elapsed,phase,devices)
        if self.stream.isatty():
            width = max(20,shutil.get_terminal_size((120,24)).columns-1)
            lines = wrap_tokens(tokens,width)
            if self.previous_rows:
                self.stream.write('\r')
                for _ in range(self.previous_rows-1):
                    self.stream.write('\x1b[2K\x1b[1A\r')
                self.stream.write('\x1b[2K\r')
            self.stream.write('\n'.join(lines))
            self.previous_rows = len(lines)
            if final:
                self.stream.write('\n')
                self.previous_rows = 0
        else:
            self.stream.write(' '.join(tokens)+'\n')
        self.stream.flush()
        self.last = now
        return True

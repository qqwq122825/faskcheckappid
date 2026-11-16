"""Mutually exclusive result counts and a one-second terminal status panel."""
from difflib import SequenceMatcher
import shutil
import sys
import time
import unicodedata


_APPLE_REASON_BUCKETS = (
    ('unable to sign you in to your apple account. try again later', 'timeout'),
    ('this request could not be completed because of a server error', 'timeout'),
    ('this action cannot be completed', 'timeout'),
    ('this apple account has been locked for security reasons', 'locked'),
    ('this apple account has been locked', 'locked'),
    ('enter the correct password for this apple account', 'wrong_password'),
    ('this apple account is not active', 'invalid'),
    ('your account information was entered incorrectly', 'not_found'),
)


def _normalized_apple_reason(*values):
    """Normalize Apple prose without changing the raw reason we persist."""
    text = ' '.join(str(value).strip() for value in values if value)
    return ' '.join(unicodedata.normalize('NFKC', text).split()).casefold()


def _apple_reason_matches(text, phrase, threshold=0.90):
    if phrase in text:
        return True
    # Apple reasons start the field; compare the phrase-sized prefix so an
    # official help suffix (for example iForgot) cannot dilute similarity.
    candidate = text[:len(phrase)]
    return bool(candidate and SequenceMatcher(
        None, phrase, candidate, autojunk=False).ratio() >= threshold)


def apple_reason_bucket(result):
    """Classify only explicit English account reasons returned by Apple."""
    values = (result.get('apple_message'), result.get('reason'), result.get('detail'))
    texts = [_normalized_apple_reason(value) for value in values if value]
    for phrase, bucket in _APPLE_REASON_BUCKETS:
        if any(_apple_reason_matches(text, phrase) for text in texts):
            return bucket
    return None


def result_bucket(result):
    status = result.get('status')
    try:
        apple_ec = int(result.get('apple_ec'))
    except (TypeError, ValueError):
        apple_ec = None
    if result.get('kind') == 'timeout' or status == 'timeout':
        return 'timeout'
    # 请求没有拿到任何 HTTP 响应时，协议层统一记录 http=0。它不是业务
    # 返回，也不应该落入“其他未验证”；按用户可理解的口径归为超时。
    try:
        explicit_http_zero = 'http' in result and result.get('http') is not None and int(result['http']) == 0
    except (TypeError, ValueError):
        explicit_http_zero = False
    if explicit_http_zero:
        return 'timeout'
    # 账号业务分类以 Apple 返回的英文原因为准，不用可复用于多种
    # 原因的 ec 错误码猜测账号状态。-22421 仅是设备环境异常的重试信号。
    reason_bucket = apple_reason_bucket(result)
    if reason_bucket:
        return reason_bucket
    if apple_ec == -22421:
        return 'timeout'
    if status == 'need_2fa':
        return 'need_2fa'
    if status == 'valid' or (status == 'needs_setup' and result.get('password_verified') is True):
        return 'valid'
    return 'undetermined'


def snapshot(total, results, active=0):
    counts = {key: 0 for key in ('valid','wrong_password','need_2fa','locked','timeout','invalid','not_found','undetermined')}
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
              f'密码正确:{counts["valid"]}',f'密码错误:{counts["wrong_password"]}',
              f'双重:{counts["need_2fa"]}',f'锁定:{counts["locked"]}',
              f'请重试:{counts["timeout"]}',f'未激活:{counts["invalid"]}',
              f'不存在:{counts["not_found"]}',
              f'其他错误:{counts["undetermined"]}',
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

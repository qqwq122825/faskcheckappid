"""Build bounded diagnostic ZIPs without copying private records verbatim."""
import io
import json
import zipfile


LOG_ARCHIVE_LIMIT = 64 * 1024 * 1024
LOG_FILES = (
    'events.jsonl', 'results.jsonl', 'summary.json', 'debug.jsonl',
    'debug-summary.json', 'state.json', 'exports/undetermined-details.jsonl',
)
_PRIVATE_FIELDS = {
    'account', 'email', 'username', 'authorization', 'proxy_authorization',
    'cookie', 'cookies', 'set_cookie', 'proxy', 'proxies', 'proxy_api',
    'proxy_auth', 'api_url', 'headers', 'request_headers', 'response_headers',
    'request_body', 'response_body', 'identity', 'device_key',
}
_OMITTED = {'event': 'log_record_omitted', 'reason': 'invalid_json'}


class LogArchiveTooLarge(ValueError):
    pass


def _redact(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            normalized = key.lower().replace('-', '_')
            if (normalized in _PRIVATE_FIELDS
                    or any(part in normalized for part in
                           ('password', 'passwd', 'credential', 'secret', 'token'))):
                continue
            result[key] = _redact(item)
        return result
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _diagnostic_record(raw):
    try:
        value = json.loads(raw)
        # Log records and summaries are objects. Never forward arbitrary text,
        # including a partial final write, on a JSON decode failure.
        if not isinstance(value, dict):
            return _OMITTED
        return _redact(value)
    except (ValueError, UnicodeError, RecursionError):
        return _OMITTED


def build_log_archive(directory, limit=LOG_ARCHIVE_LIMIT):
    sources = [(relative, directory / relative) for relative in LOG_FILES
               if (directory / relative).is_file()]
    if sum(path.stat().st_size for _, path in sources) > limit:
        raise LogArchiveTooLarge()
    buffer = io.BytesIO()
    consumed = written = 0
    try:
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
            for relative, path in sources:
                # Preserve the public ZIP member name used by existing clients.
                name = path.name if relative.startswith('exports/') else relative
                with path.open('rb') as source, archive.open(name, 'w') as output:
                    while True:
                        remaining = max(0, limit - consumed)
                        raw = (source.readline(remaining + 1)
                               if path.suffix == '.jsonl'
                               else source.read(remaining + 1))
                        if not raw:
                            break
                        consumed += len(raw)
                        if consumed > limit:
                            raise LogArchiveTooLarge()
                        encoded = (json.dumps(_diagnostic_record(raw), ensure_ascii=False,
                                              separators=(',', ':')) + '\n').encode('utf-8')
                        written += len(encoded)
                        if written > limit:
                            raise LogArchiveTooLarge()
                        output.write(encoded)
                        if path.suffix != '.jsonl':
                            break
        buffer.seek(0)
        return buffer
    except BaseException:
        buffer.close()
        raise

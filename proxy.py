from flask import Flask, request, Response
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from postgrest.exceptions import APIError
from supabase import create_client, Client

app = Flask(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://hvbupohxkifdqgzxiwhe.supabase.co').strip()
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh2YnVwb2h4a2lmZHFnenhpd2hlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjAxMDU1NTAsImV4cCI6MjA3NTY4MTU1MH0.AEpFQAgIESzKxEuVC7f5y9NbWIYuJGlJ4YfeMztfB3o').strip()

_supabase: Optional[Client] = None


def get_supabase() -> Optional[Client]:
    global _supabase
    if _supabase is not None:
        return _supabase
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        print('[iclock/cdata] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing; DB ops skipped')
        return None
    _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _supabase


def text_response(body: str, status: int = 200) -> Response:
    return Response(body, status=status, mimetype='text/plain')


# --- Helper Functions to map raw hardware codes to readable text ---
def get_verify_method(type_code: int) -> str:
    methods = {
        0: 'Password',
        1: 'Fingerprint',
        3: 'Password',
        4: 'RFID Card',
        15: 'Face',
        200: 'Others',
    }
    return methods.get(type_code, 'Unknown')


def get_punch_type(status_code: int) -> str:
    statuses = {
        0: 'check-in',
        1: 'check-out',
        2: 'break-out',
        3: 'break-in',
        4: 'overtime-in',
        5: 'overtime-out',
    }
    return statuses.get(status_code, 'check-in')


def preprocess_biometric_iclock_line(table: str, line: str) -> str:
    """BIODATA lines often start with 'BIODATA Pin=...' (space) or 'BIODATA\\t...'."""
    s = line.strip()
    if table != 'BIODATA':
        return line
    upper = s.upper()
    if upper.startswith('BIODATA\t'):
        return s[8:].lstrip()
    if upper.startswith('BIODATA '):
        return s[8:].lstrip()
    return line


def zk_push_biodata_type_to_biometric_type(type_code: int) -> str:
    """Map ZK push BIODATA Type to essl_biometrics.biometric_type (firmware-dependent)."""
    if type_code == 9:
        return 'FACE'
    if 0 <= type_code <= 8:
        return 'FINGERPRINT'
    return f'ZK_BIODATA_{type_code}'


def fetch_essl_biometrics_for_pins(
    supabase: Client,
    gym_id: str,
    essl_id_list: list,
) -> tuple:
    if not essl_id_list:
        return [], None

    def run_in(ids: list) -> tuple:
        try:
            q = (
                supabase.table('essl_biometrics')
                .select('id, essl_id, user_id, profile_gym_id')
                .eq('gym_id', gym_id)
                .in_('essl_id', ids)
                .execute()
            )
            return (q.data or []), None
        except APIError as e:
            return None, e

    rows, err = run_in(essl_id_list)
    if err is not None and all(re.fullmatch(r'\d+', s) for s in essl_id_list):
        nums = [int(s, 10) for s in essl_id_list]
        return run_in(nums)
    return rows, err


def punch_timestamp_to_iso(timestamp: Optional[str]) -> Optional[str]:
    if not timestamp:
        return None
    s = timestamp.strip()
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(s.replace(' ', 'T', 1))
    except ValueError:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def handle_iclock_cdata_get() -> Response:
    sn = request.args.get('SN')
    table = request.args.get('table')
    print('[iclock/cdata GET]', {
        'sn': sn,
        'table': table,
        'note': 'GET is handshake only; ATTLOG body is ignored. Use POST for punch data.',
    })

    sb = get_supabase()
    if sn and sb:
        try:
            res = (
                sb.table('gym_devices')
                .update({
                    'status': 'online',
                    'last_ping_at': datetime.now(timezone.utc).isoformat(),
                })
                .eq('serial_number', sn)
                .execute()
            )
            data = res.data or []
            if not data:
                print(
                    f'[iclock/cdata GET] no gym_devices row matched serial_number={sn!r} (update skipped)'
                )
            else:
                print('[iclock/cdata GET] gym_devices marked online, device id:', data[0].get('id'))
        except APIError as update_err:
            print('[iclock/cdata GET] gym_devices update error:', update_err)

        # Oldest pending command for this machine (same as Next.js /iclock/cdata GET)
        try:
            cmd_res = (
                sb.table('device_commands')
                .select('*')
                .eq('device_sn', sn)
                .eq('status', 'pending')
                .order('id', desc=False)
                .limit(1)
                .execute()
            )
            cmd_rows = cmd_res.data or []
            if cmd_rows:
                command = cmd_rows[0]
                formatted_command = f"C:{command['id']}:{command['command_string']}\n"
                print(f'>>> SENDING COMMAND TO {sn}:', formatted_command.strip())
                sb.table('device_commands').update({'status': 'sent'}).eq(
                    'id', command['id']
                ).execute()
                return text_response(formatted_command)
        except APIError as cmd_err:
            print('[iclock/cdata GET] device_commands error:', cmd_err)
        except Exception as cmd_ex:
            print('[iclock/cdata GET] device_commands exception:', cmd_ex)

    return text_response('OK')


def dedupe_attendance_logs(logs_to_insert: list, window_ms: int = 10_000) -> list:
    grouped: dict = {}
    for log in logs_to_insert:
        key = (log.get('essl_id') or '').strip() or f'__no_id_{uuid.uuid4()}'
        grouped.setdefault(key, []).append(log)

    result: list = []
    for essl_id, punches in grouped.items():
        if essl_id.startswith('__no_id_') or len(punches) == 1:
            result.extend(punches)
            continue

        punches.sort(
            key=lambda r: datetime.fromisoformat(r['punch_time'].replace('Z', '+00:00')).timestamp()
        )
        cluster_latest = punches[0]
        for i in range(1, len(punches)):
            gap_ms = (
                datetime.fromisoformat(punches[i]['punch_time'].replace('Z', '+00:00')).timestamp()
                - datetime.fromisoformat(cluster_latest['punch_time'].replace('Z', '+00:00')).timestamp()
            ) * 1000
            if gap_ms <= window_ms:
                cluster_latest = punches[i]
            else:
                result.append(cluster_latest)
                cluster_latest = punches[i]
        result.append(cluster_latest)
    return result


def handle_iclock_cdata_post() -> Response:
    sn = request.args.get('SN')
    table = request.args.get('table')
    raw_data = request.get_data(as_text=True)

    raw_preview = (
        raw_data[:400]
        .replace('\t', '\\t')
        .replace('\r', '\\r')
        .replace('\n', '\\n')
    )
    print('[iclock/cdata POST] incoming', {
        'sn': sn,
        'table': table,
        'bodyLength': len(raw_data),
        'bodyPreview': raw_preview,
        'hasTabDelimiters': '\t' in raw_data,
    })

    if not sn:
        print('[iclock/cdata POST] missing SN query param; returning OK without processing')
        return text_response('OK')

    sb = get_supabase()

    # ---------------------------------------------------------
    # HANDLE ATTENDANCE LOGS (ATTLOG)
    # ---------------------------------------------------------
    if table == 'ATTLOG':
        if not sb:
            print('[iclock/cdata ATTLOG] Supabase not configured; skipping DB work')
        else:
            try:
                res = (
                    sb.table('gym_devices')
                    .select('id, gym_id')
                    .eq('serial_number', sn)
                    .execute()
                )
                rows = res.data or []
                if len(rows) != 1:
                    print('[iclock/cdata ATTLOG] device lookup failed', {
                        'sn': sn,
                        'rowCount': len(rows),
                    })
                    return text_response('OK')
                device = rows[0]

                print('[iclock/cdata ATTLOG] device resolved', {
                    'deviceId': device['id'],
                    'gymId': device['gym_id'],
                })

                lines = raw_data.split('\n')
                parsed_punches: List[Dict[str, Any]] = []
                essl_ids_to_lookup: Set[str] = set()

                print('[iclock/cdata ATTLOG] split lines', {
                    'lineCount': len(lines),
                    'nonEmptyLines': len([l for l in lines if l.strip()]),
                })

                for line in lines:
                    line = line.rstrip('\r')
                    if not line.strip():
                        continue
                    parts = line.split('\t')
                    essl_id_raw = parts[0] if len(parts) > 0 else ''
                    timestamp = parts[1] if len(parts) > 1 else None
                    status_s = parts[2] if len(parts) > 2 else '0'
                    verify_s = parts[3] if len(parts) > 3 else '0'
                    essl_id = (essl_id_raw or '').strip()

                    if len(parts) < 4:
                        print(
                            '[iclock/cdata ATTLOG] line has wrong field count (need tab-separated esslId, time, status, verify)',
                            {
                                'fieldCount': len(parts),
                                'preview': line[:120].replace('\t', '\\t'),
                                'hint': "If you used spaces in curl, switch to $'...\\t...\\t...'",
                            },
                        )

                    try:
                        status = int(status_s, 10)
                    except ValueError:
                        status = 0
                    try:
                        verify_type = int(verify_s, 10)
                    except ValueError:
                        verify_type = 0

                    parsed_punches.append({
                        'esslId': essl_id,
                        'timestamp': timestamp,
                        'status': status,
                        'verifyType': verify_type,
                    })
                    if essl_id:
                        essl_ids_to_lookup.add(essl_id)

                print('[iclock/cdata ATTLOG] parsed punches', {
                    'count': len(parsed_punches),
                    'esslIds': list(essl_ids_to_lookup),
                    'sample': parsed_punches[0] if parsed_punches else None,
                })

                essl_id_list = list(essl_ids_to_lookup)
                users_info, bio_error = fetch_essl_biometrics_for_pins(
                    sb, device['gym_id'], essl_id_list
                )

                if bio_error:
                    print('[iclock/cdata ATTLOG] essl_biometrics lookup error:', bio_error)
                else:
                    print('[iclock/cdata ATTLOG] essl_biometrics matches', {
                        'requested': len(essl_ids_to_lookup),
                        'matchedRows': len(users_info or []),
                    })

                bio_by_essl_id: Dict[str, Dict[str, Optional[str]]] = {}
                if users_info:
                    for u in users_info:
                        key = str(u.get('essl_id', '')).strip()
                        if key in bio_by_essl_id:
                            print('[iclock/cdata ATTLOG] duplicate essl_id in essl_biometrics for gym', {
                                'gymId': device['gym_id'],
                                'essl_id': key,
                            })
                        bio_by_essl_id[key] = {
                            'user_id': u.get('user_id'),
                            'profile_gym_id': u.get('profile_gym_id'),
                        }

                logs_to_insert: List[Dict[str, Any]] = []
                skipped_no_profile = 0
                for punch in parsed_punches:
                    punch_time = punch_timestamp_to_iso(punch.get('timestamp'))
                    eid = (punch.get('esslId') or '').strip()
                    resolved = bio_by_essl_id.get(eid) if eid else None
                    r = resolved or {}
                    # Only persist when essl_biometrics maps to user (profile) or profile_gym
                    if not (r.get('user_id') or r.get('profile_gym_id')):
                        skipped_no_profile += 1
                        continue
                    logs_to_insert.append({
                        'gym_id': device['gym_id'],
                        'device_id': device['id'],
                        'user_id': r.get('user_id'),
                        'profile_gym_id': r.get('profile_gym_id'),
                        'essl_id': eid or None,
                        'punch_time': punch_time,
                        'punch_type': get_punch_type(punch['status']),
                        'verify_method': get_verify_method(punch['verifyType']),
                        'raw_status': punch['status'],
                        'raw_verify_type': punch['verifyType'],
                        'is_manual_entry': False,
                    })

                if skipped_no_profile > 0:
                    print('[iclock/cdata ATTLOG] skipped punches (no user_id or profile_gym_id)', {
                        'skippedNoProfile': skipped_no_profile,
                    })

                before_time_filter = len(logs_to_insert)
                logs_to_insert = [r for r in logs_to_insert if r.get('punch_time')]
                skipped_bad_time = before_time_filter - len(logs_to_insert)
                if skipped_bad_time > 0:
                    print('[iclock/cdata ATTLOG] skipped rows with invalid punch_time', {
                        'skippedBadTime': skipped_bad_time,
                    })

                deduplicated = dedupe_attendance_logs(logs_to_insert, window_ms=10_000)
                dedup_skipped = len(logs_to_insert) - len(deduplicated)
                if dedup_skipped > 0:
                    print(
                        f'[iclock/cdata ATTLOG] deduped {dedup_skipped} punch(es) '
                        f'(same essl_id within 10s → kept latest only)'
                    )

                print('[iclock/cdata ATTLOG] rows to insert', {
                    'count': len(deduplicated),
                    'sampleRow': deduplicated[0] if deduplicated else None,
                })

                if deduplicated:
                    try:
                        ins = sb.table('attendance_logs').insert(deduplicated).execute()
                        inserted = ins.data or []
                        print('[iclock/cdata ATTLOG] insert ok', {
                            'insertedCount': len(inserted) if inserted else len(deduplicated),
                            'gymId': device['gym_id'],
                        })
                    except APIError as insert_error:
                        print('[iclock/cdata ATTLOG] insert failed:', insert_error)
                else:
                    print('[iclock/cdata ATTLOG] no rows to insert (empty or unparsed body)')

            except Exception as err:
                print('[iclock/cdata ATTLOG] exception:', err)

    # ---------------------------------------------------------
    # BIOMETRIC TEMPLATES (FINGERTMP / FPINFO / FACE / BIODATA)
    # ---------------------------------------------------------
    elif table in ('FINGERTMP', 'FPINFO', 'FACE', 'BIODATA'):
        if not sb:
            print(f'[iclock/cdata {table}] Supabase not configured; skipping DB work')
        else:
            try:
                res = (
                    sb.table('gym_devices')
                    .select('gym_id')
                    .eq('serial_number', sn)
                    .execute()
                )
                drows = res.data or []
                if len(drows) != 1:
                    print(f'[iclock/cdata {table}] unknown device SN, cannot save biometrics', {
                        'sn': sn,
                        'rowCount': len(drows),
                    })
                    return text_response('OK')
                dev = drows[0]

                lines = raw_data.split('\n')
                biometrics_to_insert: List[Dict[str, Any]] = []
                essl_ids_to_lookup: Set[str] = set()

                for line in lines:
                    line = line.rstrip('\r')
                    if not line.strip():
                        continue
                    line = preprocess_biometric_iclock_line(table, line)
                    if not line.strip():
                        continue
                    pairs = line.split('\t')
                    bio_obj: Dict[str, str] = {}
                    for pair in pairs:
                        eq = pair.find('=')
                        if eq == -1:
                            continue
                        k = pair[:eq].strip()
                        v = pair[eq + 1 :].strip()
                        if k:
                            bio_obj[k.upper()] = v

                    if bio_obj.get('PIN') and bio_obj.get('TMP'):
                        pin = bio_obj['PIN'].strip()
                        if table == 'FACE':
                            biometric_type = 'FACE'
                        elif table == 'BIODATA':
                            try:
                                type_code = int(bio_obj.get('TYPE') or '0', 10)
                            except ValueError:
                                type_code = 0
                            biometric_type = zk_push_biodata_type_to_biometric_type(
                                type_code
                            )
                        else:
                            biometric_type = 'FINGERPRINT'
                        fid_raw = bio_obj.get('FID') or bio_obj.get('INDEX') or '0'
                        fid = int(fid_raw, 10)
                        try:
                            size = int(bio_obj.get('SIZE') or '0', 10)
                        except ValueError:
                            size = 0
                        if size == 0 and bio_obj.get('TMP'):
                            size = len(bio_obj['TMP'])
                        try:
                            valid = int(bio_obj.get('VALID') or '1', 10)
                        except ValueError:
                            valid = 1
                        biometrics_to_insert.append({
                            'gym_id': dev['gym_id'],
                            'essl_id': pin,
                            'biometric_type': biometric_type,
                            'fid': fid,
                            'size': size,
                            'valid': valid,
                            'tmp': bio_obj['TMP'],
                            'user_id': None,
                        })
                        essl_ids_to_lookup.add(pin)

                pin_list = list(essl_ids_to_lookup)
                users_info, _ = fetch_essl_biometrics_for_pins(sb, dev['gym_id'], pin_list)
                user_map: Dict[str, str] = {}
                if users_info:
                    for u in users_info:
                        uid = u.get('user_id')
                        if uid:
                            user_map[str(u.get('essl_id', '')).strip()] = uid

                final_insert_data = [
                    {**b, 'user_id': user_map.get(b['essl_id'])}
                    for b in biometrics_to_insert
                ]

                if final_insert_data:
                    try:
                        sb.table('essl_biometrics').upsert(
                            final_insert_data,
                            on_conflict='gym_id,essl_id,biometric_type,fid',
                        ).execute()
                        print(f'[iclock/cdata {table}] saved {len(final_insert_data)} template(s)')
                    except APIError as insert_error:
                        print(f'[iclock/cdata {table}] upsert failed:', insert_error)
            except Exception as err:
                print(f'[iclock/cdata {table}] exception:', err)

    # ---------------------------------------------------------
    # USER PROFILES (USERINFO)
    # ---------------------------------------------------------
    elif table == 'USERINFO':
        print(f'[iclock/cdata POST] USERINFO bulk sync, bodyLength={len(raw_data)}')
    elif table == 'OPERLOG':
        print(f'[iclock/cdata POST] OPERLOG, sn={sn!r}, bodyLength={len(raw_data)}')
        return text_response('OK')
    else:
        print('[iclock/cdata POST] unhandled or missing table param', {
            'table': table,
            'sn': sn,
            'bodyLength': len(raw_data),
        })

    return text_response('OK1')


@app.route('/iclock/cdata', methods=['GET', 'POST'], strict_slashes=False)
@app.route('/iclock/cdata.aspx', methods=['GET', 'POST'], strict_slashes=False)
def iclock_cdata():
    if request.method == 'GET':
        return handle_iclock_cdata_get()
    return handle_iclock_cdata_post()


@app.route('/iclock/getrequest', methods=['GET'], strict_slashes=False)
@app.route('/iclock/getrequest.aspx', methods=['GET'], strict_slashes=False)
def iclock_getrequest():
    return handle_iclock_cdata_get()


if __name__ == '__main__':
    # Render provides the port automatically via the PORT environment variable
    port = int(os.environ.get('PORT', 10000))
    print(f'🚀 iclock/cdata service on port {port}...')
    app.run(host='0.0.0.0', port=port)

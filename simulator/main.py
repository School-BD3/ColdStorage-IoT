import json
import os
import random
import ssl
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

CHAMBER_ID = int(os.environ.get('CHAMBER_ID', '1'))
HOST = os.environ.get('MQTT_HOST', 'mqtt')
PORT = int(os.environ.get('MQTT_PORT', '8883'))
USERNAME = f'chamber{CHAMBER_ID}'
PASSWORD = os.environ['MQTT_PASSWORD']
CA_FILE = os.environ.get('MQTT_CA', '/app/certs/ca.crt')
BASE = f'coldstorage/chamber{CHAMBER_ID}'
INTERVAL = float(os.environ.get('SIM_INTERVAL', '5'))
# For demonstrations only. Default: real seconds (120 and 300).
TIME_SCALE = float(os.environ.get('SIM_TIME_SCALE', '1'))
if TIME_SCALE <= 0:
    raise ValueError('SIM_TIME_SCALE must be positive')

lock = threading.Lock()
state = {
    'temperature': round(random.uniform(3.6, 4.4), 2),
    'door_open': False,
    'mains_on': True,
    'compressor_on': False,
    'alarm_on': False,
    'door_since': None,
    'temp_since': None,
    'power_since': None,
    'door_warning_sent': False,
    'door_alarm_sent': False,
    'temp_alarm_sent': False,
    'power_warning_sent': False,
}

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

def publish(client, topic, data, retain=False):
    payload = json.dumps({'ts': now_iso(), **data}, ensure_ascii=False)
    client.publish(f'{BASE}/{topic}', payload, qos=1, retain=retain)

def log_event(client, code, severity, message):
    event = {'code': code, 'severity': severity, 'message': message, 'chamber': CHAMBER_ID}
    publish(client, 'events/telemetry', event)
    print(f'[EVENT] {event}', flush=True)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f'MQTT connection failed: {reason_code}', flush=True)
        return
    client.subscribe([(f'{BASE}/+/set', 1)])
    publish(client, 'temperature/status', {'status': 'online'}, retain=True)
    print(f'Chamber {CHAMBER_ID} MQTT connected', flush=True)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        command = data.get('value')
        device = msg.topic.split('/')[-2]
        if device not in ('door', 'mains', 'compressor', 'alarm'):
            return
        if not isinstance(command, bool):
            print(f'Ignored invalid {device} command: value must be boolean', flush=True)
            return
        with lock:
            if device == 'door':
                state['door_open'] = command
                state['door_since'] = time.monotonic() if command else None
                state['door_warning_sent'] = False
                state['door_alarm_sent'] = False
                result = state['door_open']
            elif device == 'mains':
                state['mains_on'] = command
                state['power_since'] = None if command else time.monotonic()
                state['power_warning_sent'] = False
                if not command:
                    state['compressor_on'] = False
                result = state['mains_on']
            elif device == 'compressor':
                state['compressor_on'] = command and state['mains_on']
                result = state['compressor_on']
            else:
                state['alarm_on'] = command
                result = state['alarm_on']
        publish(client, f'{device}/state', {'value': result}, retain=True)
        log_event(client, f'{device.upper()}_COMMAND', 'info', f'{device} set to {result}')
    except (ValueError, UnicodeDecodeError, IndexError) as exc:
        print(f'Invalid MQTT command: {exc}', flush=True)

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'coldstorage-chamber-{CHAMBER_ID}', protocol=mqtt.MQTTv5)
    client.username_pw_set(USERNAME, PASSWORD)
    client.tls_set(ca_certs=CA_FILE, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.will_set(f'{BASE}/temperature/status', json.dumps({'status': 'offline'}), qos=1, retain=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, keepalive=30)
    client.loop_start()
    try:
        while True:
            events = []
            with lock:
                s = state
                now = time.monotonic()
                if s['mains_on']:
                    if s['compressor_on']:
                        s['temperature'] -= random.uniform(.06, .13)
                    else:
                        s['temperature'] += random.uniform(.02, .07)
                else:
                    s['temperature'] += random.uniform(.04, .12)
                if s['door_open']:
                    s['temperature'] += random.uniform(.07, .16)
                s['temperature'] = round(s['temperature'], 2)

                # Automatic control in the simulator, with hysteresis.
                if s['mains_on']:
                    if s['temperature'] >= 5.0:
                        s['compressor_on'] = True
                    elif s['temperature'] <= 3.0:
                        s['compressor_on'] = False
                else:
                    s['compressor_on'] = False

                if s['door_open'] and s['door_since'] is not None:
                    elapsed = (now - s['door_since']) * TIME_SCALE
                    if elapsed >= 120 and not s['door_warning_sent']:
                        events.append(('DOOR_WARNING', 'warning', 'Door open for more than 2 minutes'))
                        s['door_warning_sent'] = True
                    if elapsed >= 300 and not s['door_alarm_sent']:
                        events.append(('DOOR_ALARM', 'alarm', 'Door open for more than 5 minutes'))
                        s['door_alarm_sent'] = True
                else:
                    s['door_warning_sent'] = False
                    s['door_alarm_sent'] = False

                if not 2 <= s['temperature'] <= 8:
                    if s['temp_since'] is None:
                        s['temp_since'] = now
                    if (now - s['temp_since']) * TIME_SCALE >= 300 and not s['temp_alarm_sent']:
                        events.append(('TEMPERATURE_ALARM', 'alarm', 'Temperature outside 2-8 C for more than 5 minutes'))
                        s['temp_alarm_sent'] = True
                else:
                    s['temp_since'] = None
                    s['temp_alarm_sent'] = False

                if not s['mains_on'] and not s['power_warning_sent']:
                    events.append(('POWER_OUTAGE', 'warning', 'Mains unavailable; do not open doors'))
                    s['power_warning_sent'] = True
                if s['mains_on']:
                    s['power_warning_sent'] = False
                s['alarm_on'] = s['door_alarm_sent'] or s['temp_alarm_sent']
                snapshot = {
                    'temperature': s['temperature'],
                    'door': s['door_open'],
                    'mains': s['mains_on'],
                    'compressor': s['compressor_on'],
                    'alarm': s['alarm_on'],
                    'power_outage_seconds': round(now - s['power_since']) if s['power_since'] else 0,
                }
            publish(client, 'temperature/telemetry', {'value': snapshot['temperature'], 'unit': 'C'})
            publish(client, 'door/telemetry', {'value': snapshot['door']})
            publish(client, 'mains/telemetry', {'value': snapshot['mains'], 'outage_seconds': snapshot['power_outage_seconds']})
            publish(client, 'compressor/state', {'value': snapshot['compressor']}, retain=True)
            publish(client, 'alarm/state', {'value': snapshot['alarm']}, retain=True)
            for event in events:
                log_event(client, *event)
            print(f"Chamber {CHAMBER_ID}: {snapshot}", flush=True)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        publish(client, 'temperature/status', {'status': 'offline'}, retain=True)
        client.disconnect()
        client.loop_stop()

if __name__ == '__main__':
    main()
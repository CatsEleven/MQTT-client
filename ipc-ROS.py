import os
import json
import uuid
import time
from datetime import datetime
import paho.mqtt.client as mqtt
from multiprocessing.connection import Listener

# ----------------------------------------------------
# 設定 (Settings)
# ----------------------------------------------------
# MQTT
BROKER_ADDRESS = "broker.hivemq.com"
PORT = 1883
TOPIC_BINARY = "binaryChunks"
TOPIC_JSON   = "telemetry/data"
CHUNK_SIZE = 100 * 1024  # 100KB

# IPC (ROSノードと同じ設定にする)
IPC_ADDRESS = ('localhost', 6000)
IPC_AUTHKEY = b'secret'

# ----------------------------------------------------
# MQTT Callbacks (Version 1 Style)
# ----------------------------------------------------

# Version 1 の引数: (client, userdata, flags, rc)
def on_connect(client, userdata, flags, rc):
    # rc=0 が成功
    status = "Success" if rc == 0 else f"Error code {rc}"
    print(f"[MQTT] Connected! ({status})")

# Version 1 の引数: (client, userdata, mid)
def on_publish(client, userdata, mid):
    # デバッグ用
    # print(f"[MQTT] Published (mid={mid})")
    pass

# MQTTクライアントのセットアップ
# ★ここ重要: Version 1 を明示的に指定して互換性を保つ
client = mqtt.Client(
    client_id=f"bridge_{uuid.uuid4()}",
    callback_api_version=mqtt.CallbackAPIVersion.VERSION1
)
client.on_connect = on_connect
client.on_publish = on_publish

# ----------------------------------------------------
# 送信ロジック: 画像バイナリチャンク送信
# ----------------------------------------------------
def send_file_binary_chunks(filepath):
    if not os.path.exists(filepath):
        print(f"[Error] File not found: {filepath}")
        return

    filename = os.path.basename(filepath)
    message_id = str(uuid.uuid4())

    with open(filepath, "rb") as f:
        data = f.read()

    total_size = len(data)
    total_chunks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"\n[MQTT] Sending Image: {filename} ({total_size} bytes)")

    for idx in range(total_chunks):
        start = idx * CHUNK_SIZE
        end = start + CHUNK_SIZE
        chunk_bytes = data[start:end]

        header = {
            "message_id": message_id,
            "filename": filename,
            "chunk_index": idx,
            "total_chunks": total_chunks,
            "timestamp": datetime.now().isoformat(),
            "chunk_size": len(chunk_bytes),
        }

        # JSONヘッダー + 改行 + バイナリデータ
        payload = json.dumps(header).encode("utf-8") + b"\n" + chunk_bytes

        info = client.publish(TOPIC_BINARY, payload=payload, qos=1)
        info.wait_for_publish()
    
    print(f"[MQTT] Image transfer complete: {filename}")

# ----------------------------------------------------
# 送信ロジック: テレメトリ送信
# ----------------------------------------------------
def send_telemetry(speed, gnss_x, gnss_y, source_event):
    speed = speed if speed is not None else 0.0
    lat = gnss_y if gnss_y is not None else 0.0
    lon = gnss_x if gnss_x is not None else 0.0

    telemetry = {
        "event": source_event,
        "speed": speed,
        "latitude": lat,
        "longitude": lon,
        "timestamp": datetime.now().isoformat(),
    }

    payload = json.dumps(telemetry, ensure_ascii=False)
    info = client.publish(TOPIC_JSON, payload=payload, qos=1)
    info.wait_for_publish()

    print(f"[MQTT] Telemetry sent: {payload}")

# ----------------------------------------------------
# メイン処理 (IPC Server -> MQTT Bridge)
# ----------------------------------------------------
def main():
    # 1. MQTT接続開始
    try:
        client.connect(BROKER_ADDRESS, PORT, 60)
        client.loop_start() 
    except Exception as e:
        print(f"MQTT Connection failed: {e}")
        return

    # 2. IPCリスナー起動
    listener = Listener(IPC_ADDRESS, authkey=IPC_AUTHKEY)
    print(f"\n[Bridge] Waiting for ROS2 Node connection on {IPC_ADDRESS}...")

    try:
        while True:
            conn = listener.accept()
            print("[Bridge] ROS2 Node connected!")

            try:
                while True:
                    msg = conn.recv()
                    
                    msg_type = msg.get('type')

                    if msg_type == 'AEB_TRIGGER':
                        print("\n>>> Event: AEB Trigger Received")
                        send_telemetry(
                            speed=msg.get('velocity'),
                            gnss_x=msg.get('gnss_x'),
                            gnss_y=msg.get('gnss_y'),
                            source_event="AEB_TRIGGER"
                        )

                    elif msg_type == 'IMAGE_SAVED':
                        path = msg.get('path')
                        print(f"\n>>> Event: Image Saved ({path})")
                        
                        gnss = msg.get('gnss')
                        send_telemetry(
                            speed=msg.get('velocity'),
                            gnss_x=gnss[0] if gnss else None,
                            gnss_y=gnss[1] if gnss else None,
                            source_event="IMAGE_CAPTURED"
                        )
                        
                        send_file_binary_chunks(path)

            except EOFError:
                print("[Bridge] ROS2 Node disconnected. Waiting for reconnection...")
            except Exception as e:
                print(f"[Bridge] Error in data loop: {e}")
            finally:
                conn.close()

    except KeyboardInterrupt:
        print("\n[Bridge] Stopping...")
    finally:
        listener.close()
        client.loop_stop()
        client.disconnect()
        print("[Bridge] Disconnected.")

if __name__ == "__main__":
    main()
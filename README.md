# 車両データを活用した子ども向け交通安全マップ
## MQTT Publisher
車両からBrokerに通信を行うMQTT Publisherです．

## 構成
- ROSノードとプロセス間通信を行います．
- MQTT BrokerにはHive MQを使います．
- 画像を分割してBrokerに送信します．

## 手順
1. このリポジトリをcloneしてください．
2. ```pip install -r requirements.txt```で依存関係をインストールします．
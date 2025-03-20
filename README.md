# shms-python
## KUKAR
```
▶ pm2 start kukar-csv-asyncnotify.py --name "CAMPBELL ASYNC" --interpreter python3
[PM2] Starting /home/shms/shms-python/kukar/kukar-csv-asyncnotify.py in fork_mode (1 instance)
[PM2] Done.
┌────┬───────────────────┬─────────────┬─────────┬─────────┬──────────┬────────┬──────┬───────────┬──────────┬──────────┬──────────┬──────────┐
│ id │ name              │ namespace   │ version │ mode    │ pid      │ uptime │ ↺    │ status    │ cpu      │ mem      │ user     │ watching │
├────┼───────────────────┼─────────────┼─────────┼─────────┼──────────┼────────┼──────┼───────────┼──────────┼──────────┼──────────┼──────────┤
│ 1  │ CAMPBELL ASYNC    │ default     │ N/A     │ fork    │ 5953     │ 0s     │ 0    │ online    │ 0%       │ 3.3mb    │ shms     │ disabled │
└────┴───────────────────┴─────────────┴─────────┴─────────┴──────────┴────────┴──────┴───────────┴──────────┴──────────┴──────────┴──────────┘
Module
┌────┬──────────────────────────────┬───────────────┬──────────┬──────────┬──────┬──────────┬──────────┬──────────┐
│ id │ module                       │ version       │ pid      │ status   │ ↺    │ cpu      │ mem      │ user     │
├────┼──────────────────────────────┼───────────────┼──────────┼──────────┼──────┼──────────┼──────────┼──────────┤
│ 0  │ pm2-logrotate                │ 2.7.0         │ 388      │ online   │ 0    │ 0%       │ 44.4mb   │ shms     │
└────┴──────────────────────────────┴───────────────┴──────────┴──────────┴──────┴──────────┴──────────┴──────────┘

▶ pm2 startup
[PM2] Init System found: systemd
[PM2] To setup the Startup Script, copy/paste the following command:
sudo env PATH=$PATH:/home/shms/.nvm/versions/node/v14.19.1/bin /home/shms/.nvm/versions/node/v14.19.1/lib/node_modules/pm2/bin/pm2 startup systemd -u shms --hp /home/shms

▶ pm2 save
[PM2] Saving current process list...
[PM2] Successfully saved in /home/shms/.pm2/dump.pm2
```
## OTHERS
```
▶ pm2 start kukar-mqtt.py --name "PDCM Kukar MQTT" --interpreter python3
▶ pm2 start kukar-csv.py --name "Campbell CSV" --interpreter python3
▶ pm2 start kukar-csv.py --name "Campbell CSV" --interpreter python3 --time
▶ pm2 start kukar-mqtt.py --name "PDCM Kukar MQTT" --interpreter python3 --time
▶ pm2 start PDCM-SungaiPute.py --name "PDCM Sungai Pute" --interpreter python3 --time --watch
▶ pm2 start PDCM-Kembar-Box1.py --name PDCM-Kembar-Box1 --interpreter python3
▶ pm2 start PDCM-Kudus.py --watch
▶ pm2 start index.py --name "Wika Lukulo" --watch
▶ pm2 start EMERALD-electric.py --name "Emerald Electric" --watch
▶ pm2 start PDCM-Martadipura-Box1.py --watch
▶ pm2 start PDCM-Kembar-Box2.py --watch
```
## InfluxDB 2.x OSS cheat sheet
### InfluxDB 2.7 does not support deleting data by field.
See: https://docs.influxdata.com/influxdb/v2/write-data/delete-data/#cannot-delete-data-by-field
### Change retention policy using CLI. Cannot use GUI because there is bug. Issue: https://github.com/influxdata/ui/issues/1607
```
▶ influx bucket update -id <bucket_ID> --retention 0 --token <TOKEN> --host http://localhost:8086 --skip-verify
```
## How to add private keys in Windows 11 using ssh-agent
```
 iqbal on  ~
 # Get-Service -Name ssh-agent | Set-Service -StartupType Manual
 iqbal on  ~
 # Start-Service ssh-agent
 iqbal on  ~
 # ssh-add C:\Users\iqbal\.ssh\id_rsa-OpenSSH-format-openwrt-private-Key
Identity added: C:\Users\iqbal\.ssh\id_rsa-OpenSSH-format-openwrt-private-Key (C:\Users\iqbal\.ssh\id_rsa-OpenSSH-format-openwrt-private-Key)
```
## How to add SSH Keys to authorized_keys file
See https://askubuntu.com/a/262074
```
 iqbal on  ~ chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys
```
## Copy folder while keeping permissions etc.
```
sudo cp -rp /home/shms/ftp/* /ftp
```

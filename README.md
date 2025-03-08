# shms-python
KUKAR
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

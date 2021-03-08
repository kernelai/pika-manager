// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

func main() {
	const usage = `
Usage:
	codis-admin [-v] --proxy=ADDR [--auth=AUTH] [config|model|stats|slots]
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --start
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --shutdown
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --log-level=LEVEL
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --fillslots=FILE [--locked]
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --reset-stats
	codis-admin [-v] --proxy=ADDR [--auth=AUTH]  --forcegc
	codis-admin [-v] --dashboard=ADDR           [config|model|stats|slots|group|proxy]
	codis-admin [-v] --dashboard=ADDR            --shutdown
	codis-admin [-v] --dashboard=ADDR            --reload
	codis-admin [-v] --dashboard=ADDR            --log-level=LEVEL
	codis-admin [-v] --dashboard=ADDR            --manager-status
	codis-admin [-v] --dashboard=ADDR            --set-manager (--enable|--disable)
	codis-admin [-v] --dashboard=ADDR            --slots-assign   --beg=ID --end=ID (--gid=ID|--offline) --tid=ID [--confirm]
	codis-admin [-v] --dashboard=ADDR            --slots-status   [--gid=ID]
	codis-admin [-v] --dashboard=ADDR            --list-proxy
	codis-admin [-v] --dashboard=ADDR            --create-proxy   --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --online-proxy   --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --remove-proxy  (--addr=ADDR|--token=TOKEN|--pid=ID)       [--force]
	codis-admin [-v] --dashboard=ADDR            --reinit-proxy  (--addr=ADDR|--token=TOKEN|--pid=ID|--all) [--force]
	codis-admin [-v] --dashboard=ADDR            --proxy-status
	codis-admin [-v] --dashboard=ADDR            --create-table   --name=table-name --num=slots-num [--tid=ID]
	codis-admin [-v] --dashboard=ADDR            --create-table-for-meta   --name=table-name --num=slots-num [--tid=ID]
	codis-admin [-v] --dashboard=ADDR            --create-table-for-pika   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --remove-table   --tid=ID   
	codis-admin [-v] --dashboard=ADDR            --remove-table-for-meta   --tid=ID   
	codis-admin [-v] --dashboard=ADDR            --remove-table-for-pika   --tid=ID   
	codis-admin [-v] --dashboard=ADDR            --pika-slaveof   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --pika-slaveof-no-one   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --add-slot-for-pika   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --del-slot-for-pika   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --list-table   
	codis-admin [-v] --dashboard=ADDR            --distribution   --tid=ID
	codis-admin [-v] --dashboard=ADDR            --rename-table  --name=new-name --tid=ID --auth=table-auth
	codis-admin [-v] --dashboard=ADDR            --get-table-meta
	codis-admin [-v] --dashboard=ADDR            --set-table-meta   --tid=ID   
	codis-admin [-v] --dashboard=ADDR            --set-table-block   --tid=ID  (--enable|--disable)  
	codis-admin [-v] --dashboard=ADDR            --list-group
	codis-admin [-v] --dashboard=ADDR            --create-group   --gid=ID
	codis-admin [-v] --dashboard=ADDR            --remove-group   --gid=ID
	codis-admin [-v] --dashboard=ADDR            --resync-group  [--gid=ID | --all]
	codis-admin [-v] --dashboard=ADDR            --group-add      --gid=ID --addr=ADDR [--datacenter=DATACENTER]
	codis-admin [-v] --dashboard=ADDR            --group-del      --gid=ID --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --group-status
	codis-admin [-v] --dashboard=ADDR            --replica-groups --gid=ID --addr=ADDR (--enable|--disable)
	codis-admin [-v] --dashboard=ADDR            --promote-server --gid=ID --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --sync-action    --create --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --sync-action    --remove --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --slot-action    --create --sid=ID --gid=ID --tid=ID
	codis-admin [-v] --dashboard=ADDR            --slot-action    --remove --sid=ID --tid=ID
	codis-admin [-v] --dashboard=ADDR            --slot-action    --create-some  --gid-from=ID --gid-to=ID --num-slots=N --tid=ID
	codis-admin [-v] --dashboard=ADDR            --slot-action    --create-range --beg=ID --end=ID --gid=ID --tid=ID
	codis-admin [-v] --dashboard=ADDR            --slot-action    --interval=VALUE
	codis-admin [-v] --dashboard=ADDR            --slot-action    --disabled=VALUE
	codis-admin [-v] --dashboard=ADDR            --rebalance     [--confirm]
	codis-admin [-v] --dashboard=ADDR            --sentinel-add   --addr=ADDR
	codis-admin [-v] --dashboard=ADDR            --sentinel-del   --addr=ADDR [--force]
	codis-admin [-v] --dashboard=ADDR            --sentinel-resync
	codis-admin [-v] --remove-lock               --product=NAME (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT)
	codis-admin [-v] --config-dump               --product=NAME (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT) [-1]
	codis-admin [-v] --config-convert=FILE
	codis-admin [-v] --config-restore=FILE       --product=NAME (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT) [--confirm]
	codis-admin [-v] --dashboard-list                           (--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT)

Options:
	-a AUTH, --auth=AUTH
	-x ADDR, --addr=ADDR
	-t TOKEN, --token=TOKEN
	-g ID, --gid=ID
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}
	log.SetLevel(log.LevelInfo)

	if d["-v"].(bool) {
		log.SetLevel(log.LevelDebug)
	}

	switch {
	case d["--proxy"] != nil:
		new(cmdProxy).Main(d)
	case d["--dashboard"] != nil:
		new(cmdDashboard).Main(d)
	default:
		new(cmdAdmin).Main(d)
	}
}

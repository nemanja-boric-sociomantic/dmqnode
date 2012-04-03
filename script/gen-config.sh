#!/bin/sh
#
# Automatic generation of configuration files for DHT nodes and DHT nodes XML
# file.
#
# This script needs some input file. A configuration template specified in the
# $configtpl variable. The file should be a regular DHT configuration file with
# the following placeholders that will be replaced for the actual values:
# {ADDR} for the server address
# {PORT} for the node port
# {MIN}  for the node minimun value
# {MAX}  for the node maximum value
#
# Also a ranges definition file is needed, specified in the $ranges variable.
# The ranges definition files should have one node definition per line
# consisting in the min and max value handled by the node in hexa. For example:
# 0x00000000 0x77777777
# 0x80000000 0xffffffff
#
# For example configuration files see the doc/ directory.
#
# The number of servers can be specified in the $nserv variable, the number of
# nodes per server in the $nnodes variable (the number of entries in the
# $ranges files should be equals to $nserv * $nnodes, and the ranges will be
# assigned sequentially). The naming scheme used for servers is $loc-N.$domain
# (where N is a number between $baseserv and $baseserv + $nserv - 1). The ports
# will be assigned using $baseport as the first one and incrementing one for
# each node in the same server. All servers have the first node using the port
# $baseport.
#
# Files are output in the $output directory, generating a tree structure like
# this: $output/$loc-N.$domain/${nodepref}M/$configdir (where M is a node
# number between 1 and $nnodes). The configuration file generated (with name
# $configname) for that node will be placed there. The global nodes XML
# configuration file will be placed in the $output dir.

loc=eu
domain=sociomantic.com
nserv=7
nnodes=4
baseport=30024
baseserv=42
outdir=output
nodesxml=dhtmemory.xml
configtpl=config.tpl.ini
ranges=ranges.txt
nodepref=mem-dht-
configdir=etc
configname=config.ini
remotedir=/srv/dht
copycmd="scp"


mkdir -p $outdir

xml=$outdir/$nodesxml

cat <<EOT > $xml
<?xml version="1.0"?>

<dhtnodes>
EOT

i=0
for s in `seq $baseserv $(($baseserv + $nserv - 1))`
do
	addr=$loc-$s.$domain
	servdir=$outdir/$addr
	for n in `seq 1 $nnodes`
	do
		nodedir=$servdir/$nodepref$n/$configdir
		mkdir -p $nodedir

		i=$(($i + 1))
		min=`sed -n -e ${i}p $ranges | cut -d' ' -f1`
		max=`sed -n -e ${i}p $ranges | cut -d' ' -f2`
		port=$(($baseport + $n - 1))
		sed -e "s/{ADDR}/$addr/; s/{PORT}/$port/;" \
			-e "s/{MIN}/$min/; s/{MAX}/$max/" $configtpl \
				> $nodedir/$configname
		cat <<EOT >> $xml
    <node>
       <address>$addr</address>
       <port>$port</port>
    </node>
EOT
	done
done

echo '</dhtnodes>' >> $xml

for s in `seq $baseserv $(($baseserv + $nserv - 1))`
do
	addr=$loc-$s.$domain
	servdir=$outdir/$addr
	for n in `seq 1 $nnodes`
	do
		cfg=$servdir/$nodepref$n/$configdir/$configname
		nodedir=$nodepref$n/$configdir
		echo $copycmd $cfg $xml $addr:$remotedir/$nodedir
	done
done


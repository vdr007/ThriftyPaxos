lib="bin/h2-1.4.187.jar:.:../../../dist/lib/glassPaxos.jar"
for file in `ls ../../../lib`
do
  lib=$lib:../../../lib/$file
done
cp bin/h2-1.4.187.jar /users/shir/oltpbench/lib/
sudo btrfs subvolume delete /test/backup*
rm /test/tpcc.mv.db
cp /test/tpcc.mv.db.$1 /test/tpcc.mv.db
##sudo rm /test/tpcc.mv.db
##sudo cp /test/tpcc.mv.db.$1 /test/tpcc.mv.db
sync
java -Xms16g -Xmx16g -cp $lib org.h2.tools.Server -tcpAllowOthers

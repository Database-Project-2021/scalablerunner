pip install rpyc
ip=`hostname -I | awk '{print $1}'`
path=`pwd`
rm -rf rpyc/
git clone https://github.com/tomerfiliba-org/rpyc.git
# tmux new-session -d -s "rpyc" python rpyc/bin/rpyc_classic.py --host $ip
chmod +x "$path/rpyc/bin/rpyc_classic.py"

if [ -z "$1" ]
then
      echo "Use RPYC On Default Port"
      nohup python -u "$path/rpyc/bin/rpyc_classic.py" --host "$ip" &>rpyc/rpyc.log&
else
      echo "Use RPYC On Custom Port: $1"
      nohup python -u "$path/rpyc/bin/rpyc_classic.py" --host "$ip" --port "$1" &>rpyc/rpyc.log&
fi
# nohup python -u "$path/rpyc/bin/rpyc_classic.py" --host "$ip" &>rpyc/rpyc.log&

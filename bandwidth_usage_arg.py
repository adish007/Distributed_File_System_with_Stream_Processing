import time
import argparse

def get_network_bytes():
    net_data = {}
    with open('/proc/net/dev', 'r') as f:
        lines = f.readlines()
    for line in lines[2:]:  
        line = line.strip()
        if not line or ':' not in line:
            continue
        iface, data = line.split(':', 1)
        iface = iface.strip()
        fields = data.strip().split()
        if len(fields) < 16:
            continue  
        recv_bytes = int(fields[0])
        trans_bytes = int(fields[8])
        net_data[iface] = {'recv_bytes': recv_bytes, 'trans_bytes': trans_bytes}
    return net_data

def main(i):
    interval = 1
    prev_data = get_network_bytes()
    time.sleep(interval)
    
    # continuously log bandwidth information
    # log_file_path = f'/home/ppillai3/machine.{i}.log'
    # with open(log_file_path, 'a') as log_file:
    try:
        while True:
            curr_data = get_network_bytes()
            for iface in curr_data.keys():
                if iface not in prev_data:
                    continue
                recv_diff = curr_data[iface]['recv_bytes'] - prev_data[iface]['recv_bytes']
                trans_diff = curr_data[iface]['trans_bytes'] - prev_data[iface]['trans_bytes']
                # convert to kilobytes per second
                recv_rate = (recv_diff) / (interval * 1024)
                trans_rate = (trans_diff) / (interval * 1024)
                if iface == "ens33":
                    print(f"{(recv_rate + trans_rate):.2f}")
                    # log_file.write(f"{(recv_rate + trans_rate):.2f}\n")
                    # log_file.flush()
            prev_data = curr_data
            time.sleep(interval)
    except KeyboardInterrupt:
        # stop logging when interrupted
        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Network monitor")
    parser.add_argument('i', type=int, help="The index for the log file (i.e. machine.{i}.log)")
    args = parser.parse_args()
    main(args.i)

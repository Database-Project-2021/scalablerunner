import os
import traceback

if __name__ == '__main__':
    base_path = '/opt/shared-disk2/db_betterRTE_ver2'
    round_name = 'round'
    report_name = 'reports'
    type_range = ['mmg', 'mmt']
    round_range = [str(r) for r in range(3)]
    rte_range = ['1'] + [str(rte) for rte in range(5, 201, 5)]
    # type_range = ['mmg']
    # round_range = [0]
    # rte_range = ['5']
    for r in round_range:
        for t in type_range:
            for rte in rte_range:
                src_path = os.path.join(base_path, f"{round_name}{r}", t, f"{t}_rte-{rte}", f"{t}_rte-{rte}")
                src_file = os.path.join(src_path, '*')
                dest_path = os.path.join(base_path, f"{round_name}{r}", t, f"{t}_rte-{rte}")
                cmd = f"mv {src_file} {dest_path}; rm -r {src_path}"
                print(cmd)
                try:
                    os.system(cmd)
                except:
                    traceback.print_exc()
                    print("Error")
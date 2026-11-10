import sys
import os
import extractApplicationReport

def main():
    reportsDirectory = '../reports/'
    for root, dirs, files in os.walk(reportsDirectory):
        flag = any('report' in f.lower() for f in files)
        if flag:
            paras = f'-d {root} -a created,delivery_prob,latency_avg -o {root[3:].replace("/", "_")}_Result'
            os.system(f'python extractApplicationReport.py {paras}')
            if root[-1:] != '_':
                pass


if __name__ == '__main__':
    main()

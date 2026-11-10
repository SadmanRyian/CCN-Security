import os
import sys

reportdir = '../reports/ReportsBundle/' + (sys.argv[1] if len(sys.argv) > 1 else '') + '/'
coopmodes = ['Rui\'s_scenario']
#traces = ['kaist', 'hcs', 'rwp'] 
traces = ['']
metrics = ['delivery_prob']
reports = ['MessageStatsReport']


def getFileName(mode, trace, metric):
    return mode + "_" + trace + "" + metric + ".txt"

def getTraceFiles(coopmodes, traces, metrics):
    aList = []
    for aMode in coopmodes:
        for aTrace in traces:
            for aMetric in metrics:
                aList.append(getFileName(aMode, aTrace, aMetric))
    return aList

def getMetricFromFile(metric, file):
    fullName = reportdir + file
    if not os.path.exists(fullName):
        print("no file: " + fullName)
        return 0
    with open(fullName, 'r', encoding='utf-8', errors='ignore') as aFile:
        for aLine in aFile:
            if metric in aLine:
                return aLine.split(' ')[1].strip()
    return 0

#-------------------- Main Function -------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python get-metric.py <bundle_folder>")
        return
    # get the input files
    files = getTraceFiles(coopmodes, traces, reports)
    print("files: " + str(files).strip())

    # for each file, get the metric
    for entry in files:
        aMetric = getMetricFromFile('delivery_prob', entry)
        if aMetric != 0:
            print(" metric " + str(aMetric))

if __name__ == "__main__":
    main()

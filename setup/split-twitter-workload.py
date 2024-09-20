from os import close
import sys

wlNameList = ['twis', 'twic', 'twit']
wlTemplate = "./twi-workloads/workload{}.spec_trans"
splitNum = int(sys.argv[1])

for n in wlNameList:
    fname = wlTemplate.format(n)
    wlFile = open(fname, "r")
    lines = wlFile.readlines()
    lineNum = len(lines)
    splitSize = lineNum / splitNum
    for i in range(splitNum):
        print(i * splitSize, (i + 1) * splitSize)
        slines = lines[int(i * splitSize): int((i + 1) * splitSize)]
        splitFname = fname + str(i)
        outFile = open(splitFname, "w")
        outFile.writelines(slines)
        outFile.close()
    wlFile.close()
